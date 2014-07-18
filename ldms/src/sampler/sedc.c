/*
 * Copyright (c) 2012 Open Grid Computing, Inc. All rights reserved.
 * Copyright (c) 2012 Sandia Corporation. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the BSD-type
 * license below:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *      Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *
 *      Neither the name of the Network Appliance, Inc. nor the names of
 *      its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written
 *      permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
/**
 * \file sedc.c
 * \brief sedc data provider.
 *
 * Reads the sedc data from a file (to be gotten via rsyslog) and writes to ldms metric sets. Notes:
 * - Currently reads the headers from a separate file
 * - Currently reads the compids from a separate file (when these items are inserted via the mysql insert, we will want a nice way to do remote assoc)
 * - Metric sets are currently added with all metric names, whether or not there is data for them.
 * - Metric sets are only added when they need to be (that is when a new component appears in the file) -- this is going to be a problem for the mysql inserter
* - Still have debugging statements, fixed size arrays. 
*/
#include <glib.h>
#include <inttypes.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include "ldms.h"
#include "ldmsd.h"

#define MAXMETRICSPERSET 100

struct fset {
  ldms_set_t sd;
  ldms_metric_t metrichandles[MAXMETRICSPERSET]; //FIXME: make this not fixed NOTE: includes component id as a metric
};
static char sedcheaders[MAXMETRICSPERSET][LDMSD_MAX_CONFIG_STR_LEN]; //FIMXE: make this not fixed NOTE: starts at zero, does not include component_id
static int metric_count = 0;  //NOTE: is the count of sedc metrics (number of headers)
static int numhosts = 0;
GHashTable* compidmap;
GHashTable* setmap;

char* dirnamex = NULL;
char* filebasename = NULL;
char currdate[20] = "";
char* setshortname = NULL;
char* filetype = NULL;
char* logfile = NULL;
int lastpos = 0;

char sedcfname[LDMSD_MAX_CONFIG_STR_LEN] = "";
FILE* sedcf = NULL; //sedcfile
FILE *mf = NULL; //header



ldms_metric_t *metric_table;
ldmsd_msg_log_f msglog;
int minindex = 2; //the min index in the header file

static pthread_mutex_t cfg_lock;
static size_t tot_meta_sz = 0;
static size_t tot_data_sz = 0;


static void printCompIdMap(gpointer key, gpointer value, gpointer user_data){
  if (strlen(logfile) > 0){
    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "<%s> <%d>", (char*)key, *(int*)value);
      fflush(outfile);
      fclose(outfile);
    }
  }
}

static int processCompIdMap(char * fname){
 //FIXME: can we have a function for this (note have to handle L0, node, though not the full
  //set of options since remote assoc will be handled at insert)? can this be a type and
  //offset or something??

  if (strlen(logfile) > 0){
    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "entered process compid map <%s>", fname);
      fflush(outfile);
      fclose(outfile);
    }
  }

  compidmap = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);

  FILE *cid = fopen(fname, "r");
  if (!cid) {
    msglog("Could not open the sedc file '%s'...exiting\n", fname);
    return ENOENT;
  }

  if (strlen(logfile) > 0){
    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "should be looking at file <%s>", fname);
      fflush(outfile);
      fclose(outfile);
    }
  }


  char lbuf[1024];
  while(fgets(lbuf, sizeof(lbuf), cid) != NULL){
    char* compname = (char*) g_malloc(20*sizeof(char));
    int* val = (int*) g_malloc(sizeof(int));
    int rc = sscanf(lbuf,"%s %d\n",compname,val);
    if (rc == 2){
      g_hash_table_replace(compidmap, (gpointer)compname, (gpointer)val);
      if (strlen(logfile) > 0){
	FILE* outfile = fopen(logfile, "a");
	if (outfile != NULL){
	  fprintf(outfile, "<%s> <%d>", compname, *val);
	  fflush(outfile);
	  fclose(outfile);
	}
      }
      numhosts++;
    } else {
      if (strlen(logfile) > 0){
	FILE* outfile = fopen(logfile, "a");
	if (outfile != NULL){
	  fprintf(outfile, "cant add <%s>\n", lbuf);
	  fflush(outfile);
	  fclose(outfile);
	}
      }
    }
  }

  fclose(cid);
  g_hash_table_foreach(compidmap, printCompIdMap, NULL);
  return 0;
}

static int processSEDCHeader(char* lbuf){
  //header will look just like that on the SEDC file( 2 extra non-metric fields)
  //split the line into tokens based on comma set
  size_t meta_sz, data_sz;

  /*
   * Process the header file to determine the metric set size.
   */

  if (strlen(logfile) > 0){
    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "%s", "determining the metric set size\n");
      fflush(outfile);
      fclose(outfile);
    }
  }

  tot_meta_sz = 0;
  tot_data_sz = 0;
  int rc = ldms_get_metric_size("component_id", LDMS_V_U64, &tot_meta_sz, &tot_data_sz);
  if (rc){
    return rc;
  }
  metric_count = 0;
  int count = 0;
  char *saveptr = NULL;
  
  char *pch = strtok_r(lbuf, ",\n", &saveptr);
  //  if (strlen(logfile) > 0){
  //  outfile = fopen(logfile, "a");
  // if (outfile != NULL){
  //  fprintf(outfile, "read <%s>\n", lbuf);
  //  fflush(outfile);
  //  fclose(outfile);
  //}
  //}
  while (pch != NULL){
    if (count >= minindex){
      // if (strlen(logfile) > 0){
      //      outfile = fopen(logfile, "a");
      // if (outfile != NULL){
      //      fprintf(outfile, "counting metric <%s>\n", pch);
      //      fflush(outfile);
      //      fclose(outfile);
      // }
      //}
	
      rc = ldms_get_metric_size(pch, LDMS_V_U64, &meta_sz, &data_sz);
      if (rc)
	return rc;
	
      tot_meta_sz += meta_sz;
      tot_data_sz += data_sz;
      sscanf(pch, "%s", sedcheaders[metric_count++]);       //strip leading spaces
    } else {
      // if (strlen(logfile) > 0){
      //      outfile = fopen(logfile, "a");
      // if (outfile != NULL){
      //      fprintf(outfile, "NOT counting metric <%s>\n", pch);
      //      fflush(outfile);
      //      fclose(outfile);
      // }
      // }
    }
    count++;
    pch = strtok_r(NULL,",\n", &saveptr);
  }

  return 0;
};

static int setdatafile(struct attr_value_list *kwl, struct attr_value_list *avl){

  dirnamex = strdup(av_value(avl,"datafiledir"));
  if (dirnamex == NULL){
    msglog("sedc: no datafiledir\n");
    return EINVAL;
  }

 filebasename = strdup(av_value(avl,"datafilebasename"));
  if (filebasename == NULL){
    free(dirnamex);
    msglog("sedc: no datafilebasename\n");
    return EINVAL;
  }

 filetype = strdup(av_value(avl,"datafiletype"));
  if (filetype == NULL){
    free(dirnamex);
    free(filebasename);
    msglog("sedc: no datafiletype\n");
    return EINVAL;
  }

  if (strlen(logfile) > 0){
    FILE *outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "dirnamex <%s> filebasename <%s> filetype<%s>\n",dirnamex, filebasename, filetype);
      fflush(outfile);
      fclose(outfile);
    }
  }


  if ((strcmp(filetype, "sedc") != 0) && (strcmp(filetype, "rsyslog"))){
    msglog("sedc: bad datafiletype\n");
    return EINVAL;
  }

  return 0;
}


static int setconfigfiles(struct attr_value_list *kwl, struct attr_value_list *avl){

  logfile = strdup(av_value(avl, "logfile")); //optional

  char* junk= strdup(av_value(avl,"headerfile"));
  if (junk == NULL){
    msglog("sedc: no headerfile\n");
    return EINVAL;
  }

  char lbuf[10240]; //how big does this have to be? 
  mf = fopen(junk, "r");
  if (!mf) {
    msglog("Could not open the sedc file '%s'...exiting\n", junk);
    free(junk);
    return ENOENT;
  }

  free(junk);

  int rc = 0;
  fseek(mf, 0, SEEK_SET);
  if (fgets(lbuf, sizeof(lbuf), mf) != NULL){
    rc = processSEDCHeader(lbuf);
  }
  if (mf) fclose(mf);
  if (rc != 0){
    return rc;
  }

  int i;
  for (i = 0; i < metric_count; i++){
    if (strlen(logfile) > 0){
      FILE *outfile;
      outfile = fopen(logfile, "a");
      if (outfile != NULL){ 
	fprintf(outfile, "header <%d> <%s>\n",i, sedcheaders[i]);
	fflush(outfile);
	fclose(outfile);
      }
    }
  }

  //compidmap
  junk = strdup(av_value(avl,"compidmap"));
  if (junk == NULL){
    msglog("sedc: no compidmap\n");
    return EINVAL;
  }

  rc = processCompIdMap(junk);
  free(junk);

  return rc;
}

static int setMSshortname(struct attr_value_list *kwl, struct attr_value_list *avl){
  setshortname = strdup(av_value(avl,"shortname"));
  if (setshortname == NULL){
    msglog("sedc: no metricset shortname\n");
    return EINVAL;
  }

  return 0;
}


static const char *usage(void)
{
  return  "    config action=setdatafile datafiledir=XXX datafilebasename=YYY datafiletype=ZZZ\n"
          "        - Set the datafile info\n"
          "        datafiledir         Directory of the datafile\n"
          "        datafilebasename    Basename of the datafile\n"
          "                            (e.g., L0_FSIO_TEMPS. will be followed by the current date_\n"
          "        filetype            sedc or rsyslog\n"
          "    config action=setconfigfiles logfile=XXX compidmap=YYY headerfile=ZZZ\n"
          "        - Set some config files\n"
          "        logfile             Logfile(optional)\n"
          "        compidmap           CompIdMap\n"
          "        headerfile          Headerfile\n"
          "    config action=setshortname shortname=XXX\n"
          "        - Set some metricset shortname\n"
          "        shortname           Shortname (no hostname)\n";
}


/**
 * \brief Configuration
 *
 * Usage:
 * - config action=setdatafile datafiledir=XXX datafilebasename=YYY datafiletype=ZZZ
 * <ul><li> Set the datafile info.
 * <ul><li> datafiledir:         Directory of the datafile
 * <li> datafilebasename:        Basename of the datafile. (e.g., L0_FSIO_TEMPS. will be followed by the current date)
 * <li> datafiletype:            sedc or rsyslog
 * </ul></ul>
 * - config action=setconfigfiles headerfile=XXX compidmap=YYY logfile=ZZZ
 * <ul><li> Set some config files
 * <ul><li> headerfile:    Headerfile
 * <ul><li> compidmap:     CompIdMap
 * <ul><li> logfile:       Logfile (optional)
 * </ul></ul>
 * - config action=setshortname shortname=XXX
 * <ul><li> Set metricset shortname
 * <ul><li> shortname:     Shortname.
 * </ul></ul>
 */
static int config(struct attr_value_list *kwl, struct attr_value_list *avl)
{

  char* action = av_value(avl, "action");
  if (!action){
    msglog("sedc: no action\n");
    return EINVAL;
  }

  int rc = 0;  
  pthread_mutex_lock(&cfg_lock);
  if (0 == strcmp(action, "setdatafile")){
    rc = setdatafile(kwl, avl);
  } else if (0 == strcmp(action, "setconfigfiles")){
    rc = setconfigfiles(kwl, avl);
  } else if (0 == strcmp(action, "setshortname")){
    rc = setMSshortname(kwl, avl);
  } else {
    msglog("sedc: Invalid configuration string '%s'\n", action);
    rc = EINVAL;
  }
  pthread_mutex_unlock(&cfg_lock);
  
  return rc;

}

static ldms_set_t get_set()
{
  //FIXME: can something work if there are multiple sets?
  //  return set;
  return NULL;
}

static int init(const char *path)
{
  //NOTE: the path that comes in is <localhost>/<user_specified_set_name_in_init>
  //because the metric sets are really the vals of the remote components and not
  //the local component, strip of the localhostname and use only the 
  //user_specified_set_name.
  //FIXME: this will need to be standardize -- can we get passed 2 parameters to
  //obviate this?

  sscanf(path,"%*[^/]/%s",setshortname);
  // if (strlen(logfile) > 0){
  //  FILE *outfile;
  //  outfile = fopen(logfile, "a");
  // if (outfile != NULL){
  //  fprintf(outfile, "shortname will be <%s>\n", setshortname);
  //  fflush(outfile);
  //  fclose(outfile);
  //}
  //}

  setmap = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

  return 0;
}

int createMetricSet(char* hostname, int compid, char* shortname){
  //create the metric set for a new compid
  char setnamechar[1024];
  int i;

  //FIXME: setname will be <hostname>/<setshortname>
  //can we get nid if we want that?
  snprintf(setnamechar,1024,"%s/%s",hostname,setshortname);

  if (strlen(logfile) > 0){
    FILE *outfile;
    outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "should be creating metric set for <%s> <%s> <%d>\n", hostname, setshortname, compid);
      fflush(outfile);
      fclose(outfile);
    }
  }

  struct fset *currfset = (struct fset*)g_malloc(sizeof(struct fset));

  /* Create a metric set of the required size */
  int rc = ldms_create_set(setnamechar, tot_meta_sz, tot_data_sz, &(currfset->sd));
  if (rc != 0){
    printf("Error %d creating metric set '%s'.\n", rc, setnamechar);
    if (strlen(logfile) > 0){
      FILE *outfile;
      outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Error %d creating metric set <%s>\n",
		rc, setnamechar);
	fflush(outfile);
	fclose(outfile);
      }
    }
    exit(1);
  } else {
    printf("Created set <%s>\n", setnamechar);
  }
	   
  ldms_metric_t currmetric =  ldms_add_metric(currfset->sd, "component_id", LDMS_V_U64);
  if (currmetric == 0){
    printf("Error creating the metric %s.\n", "component_id");
    exit(1);
  } else {
    if (strlen(logfile) > 0){
      FILE* outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Created metric component_id\n");
	fflush(outfile);
	fclose(outfile);
      }
    }
  }	
  currfset->metrichandles[0] = currmetric;

  for (i = 0; i < metric_count; i++){
    ldms_metric_t currmetric = ldms_add_metric(currfset->sd, sedcheaders[i], LDMS_V_U64);
    if (currmetric == 0){
      printf("Error creating the metric %s.\n", sedcheaders[i]);
      exit(1);
    } else {
      if (strlen(logfile) > 0){
	FILE* outfile = fopen(logfile, "a");
	fprintf(outfile, "Created metric <%s>\n",sedcheaders[i]);
	fflush(outfile);
	fclose(outfile);
      }
    }
    currfset->metrichandles[i+1] = currmetric;
    //    printf("added a metric handle to the vector\n");
  }

  //fill in the comp id
  union ldms_value v;
  v.v_u64 = compid;
  ldms_set_metric(currfset->metrichandles[0], &v);
  int* cid = (int*)g_malloc(sizeof(int));
  *cid = compid;
  g_hash_table_replace(setmap, (gpointer)cid, (gpointer)currfset);

  return 0;
};


static char* stripRsyslogHeaders(char* bufin){
 // Example output:
  //  "<"<syslog priority>">1" <timestamp> <hostname> <application> <pid> <bootsessionid> "["<msg_type>"@34]" <message>
  //the msg type will be FILESOURCE  (not FILESOURCE::METRICNAME). message vals will be the csv
  //the timestamp will not be used

  //  int imessage = 7;
  int imessage = 6;
  char* bufptr;
  int count = 0;

  char* p = strchr(bufin, ' ');
  while (p != NULL){
    if (count == imessage){
      //      return p;
      return p+1;
    }
    bufptr = p+1;
    p = strchr(bufptr, ' ');
    count++;
  }

  return NULL;
};


int processSEDCData(char* line){
  //split the line into tokens based on comma sep

  if (strlen(logfile) > 0){
    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "Entered process data <%s>\n", line);
      fflush(outfile);
      fclose(outfile);
    }
  }

  int* compid = NULL;
  struct fset* currfset = NULL;
  int valid = 0;

  int count = 0;
  char *pch = strsep(&line, ",\n");
  while (pch != NULL){
    if (strcmp(pch,"service id") == 0){ //skip if its a header
      break;
    }
    valid = 1;
    switch (count){
    case 0: //compname
      {
	compid = (int*)g_hash_table_lookup(compidmap, pch);
	if (compid == NULL){
	  msglog("Error: cannot find compname to id assoc %s\n", pch);
	  return -1;
	}
	currfset = (struct fset*)g_hash_table_lookup(setmap, compid);
	if (currfset == NULL){
	  int rc = createMetricSet(pch,*compid,setshortname);
	  if (rc != 0 ){
	    printf("Error: cannot create a metricset\n");
	    return -1;
	  }
	  currfset = (struct fset*)g_hash_table_lookup(setmap, compid);
	  if (currfset == NULL){
	    printf("Error: did not create metricset for <%s> \n", pch);
	    return -1;
	  }
	} else {
	  // if(strlen(logfile) > 0){
	  //	  FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
	  //	  fprintf(outfile, "will be using metric set for <%s>\n", pch);
	  //	  fflush(outfile);
	  //	  fclose(outfile);
	  //}
	  //}
	}
      }
      break;
    case 1: //time
      //      NOTE: we do *not* use the time and thus cannot do historical data.
      break;
    default: //its data
      {
	if (strlen(pch) == 0){
	  //NOTE: it is expected that there will be one too many because of the newline
	  // if (strlen(logfile) > 0){
	  //	  FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
	  //	  fprintf(outfile, "No data for metric <%d> not publishing value\n", (count-minindex+1));
	  //	  fflush(outfile);
	  //	  fclose(outfile);
	  //}
	  //}
	  break;
	}

	//FIXME: revisit this now that we know that empty values mean repeat of past value (would not have to put in new val but would want to bump datagn)
	char *pEnd;
	unsigned long long llval;
	llval = strtoll(pch,&pEnd,10);
	union ldms_value v;
	v.v_u64 = llval;

	// if (strlen(logfile) > 0){
	//	FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
	//	fprintf(outfile, "should be processing the data handle <%d> <%llu>\n", (count-minindex+1),llval);
	//	fflush(outfile);
	//	fclose(outfile);
	//}
	//}
	if ((count-minindex+1) == 0){
	  if (strlen(logfile) > 0){
	    FILE* outfile = fopen(logfile, "a");
	    if (outfile != NULL){
	      fprintf(outfile, "Error: should NOT be setting handle 0, which is the compid\n");
	      fflush(outfile);
	      fclose(outfile);
	    }
	  }
	  return -1;
	}
	if ((count-minindex+1) > metric_count){
	  if (strlen(logfile) > 0){
	    FILE* outfile = fopen(logfile, "a");
	    if (outfile != NULL){
	      fprintf(outfile, "Error: should NOT be setting handle <%d>, which is greater than the number of handles\n", (count-minindex+1));
	      fflush(outfile);
	      fclose(outfile);
	    }
	  }
	}
	ldms_set_metric(currfset->metrichandles[count-minindex+1], &v);
      } //default
      break;
    } //switch
    count++;
    pch = strsep(&line, ",\n");
  } //while

  //because the last one counted was the new line
  if ((count-minindex) <= metric_count){
    if (strlen(logfile) > 0){
      FILE* outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Error: Did not get enough metrics to process -- last possible handle was <%d>\n", (count-minindex)); //NOTE: subtracted extra 1
	fflush(outfile);
	fclose(outfile);
      }
    }
  } else {
    // if (strlen(logfile) > 0){
    //    FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
    //    fprintf(outfile, "Returning after checking <%d> values out of <%d>\n", (count-minindex), metric_count); //NOTE: subtracted extra 1
    //    fflush(outfile);
    //    fclose(outfile);
    // }
    //}
  }
    

  return 0;

};


static int processSEDCFile(){

  // if (strlen(logfile) > 0){
  //  FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
  //  fprintf(outfile, "Trying opening <%s> for reading\n", sedcfname);
  //  fflush(outfile);
  //  fclose(outfile);
  // }
  //}

  sedcf = fopen(sedcfname,"r");
  if (sedcf != NULL){
    char* line;
    size_t len = 0;
    ssize_t read;
    fseek(sedcf, lastpos, SEEK_CUR);

    while ( (read = getline(&line, &len, sedcf)) != -1){
      if (line[read-1] != '\n'){
	if (strlen(logfile) > 0){
	  FILE* outfile = fopen(logfile, "a");
	  if (outfile != NULL){
	    fprintf(outfile, "not a complete line. Closing <%s>\n", sedcfname);
	    fflush(outfile);
	    fclose(outfile);
	  }
	}
	//	fclose(sedcf);

	break;
      } else {
	//note: this will have the newline
	// if (strlen(logfile) > 0){
	//	FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
	//	fprintf(outfile, "read <%s> <length=%zu>\n", line, read);
	//	fflush(outfile);
	//	fclose(outfile);
	// }
	//}
	lastpos += read;
	
	char* p = NULL;
	if (strcmp(filetype, "rsyslog") == 0){
	  p = stripRsyslogHeaders(line);
	  if (p == NULL){
	    printf("Error stripping syslog headers\n");
	    exit(-1);
	  }
	} else {
	  p = line;
	}
	if (processSEDCData(p) != 0){
	  break;
	}
      }
    } //while
    free(line);
    if (sedcf) fclose(sedcf);
    return 0;
  } else {
    if (strlen(logfile) > 0){
      FILE* outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Can't open sedc file <%s> for reading\n",sedcfname);
	fflush(outfile);
	fclose(outfile);
      }
    }
  }

  return -1;

}


static int sample(void)
{
  //Currently: get the headers from the file.

  pthread_mutex_lock(&cfg_lock);

  // if (strlen(logfile) > 0){
  //  FILE* outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
  //  fprintf(outfile, "Entered sample\n");
  //  fflush(outfile);
  //  fclose(outfile);
  // }
  //}

  if (dirnamex == NULL || filebasename == NULL){
    msglog("sedc: No data file info\n");
    pthread_mutex_unlock(&cfg_lock);
    return ENOENT;
  }

  if (setshortname == NULL){
    msglog("sedc: No set shortname\n");
    pthread_mutex_unlock(&cfg_lock);
    return ENOENT;
  }

  char localdate[20];
  char command[20] = "date +%Y%m%d";

  if (strlen(sedcfname) != 0){
    processSEDCFile();
  }

  FILE *fpipe;
  if (!(fpipe = (FILE*)popen(command,"r"))){
    perror("Problems with pipe");
    if (strlen(logfile) > 0){
      FILE* outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Cant get date\n");
	fflush(outfile);
	fclose(outfile);
      }
    }
    pthread_mutex_unlock(&cfg_lock);
    return -1;
  }

  if (!fgets( localdate, sizeof localdate, fpipe)){
    perror("Problems with reading date");
    if (strlen(logfile) > 0){
      FILE* outfile = fopen(logfile, "a");
      if (outfile != NULL){
	fprintf(outfile, "Problems with reading date\n");
	fflush(outfile);
	fclose(outfile);
      }
    }
    pthread_mutex_unlock(&cfg_lock);
    return -1;
  }
  localdate[strlen(localdate)-1] = '\0';
  
  // if (strlen(logfile) > 0){
  //  outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
  //  fprintf(outfile, "Currdate <%s> localdate <%s> \n", currdate, localdate);
  //  fflush(outfile);
  //  fclose(outfile);
  //}
  //}

  if (strcmp(localdate,currdate) != 0){
    //if (strlen(logfile) > 0){
	  // if (outfile != NULL){
    //    outfile = fopen(logfile, "a");
    //    fprintf(outfile, "New date. determining new file\n");
    //    fflush(outfile);
    //    fclose(outfile);
    //}
    //}

    snprintf(sedcfname,LDMSD_MAX_CONFIG_STR_LEN-1,
	     "%s/%s-%s",dirnamex,filebasename,localdate);
    lastpos = 0;
    snprintf(currdate,9,localdate);

    FILE* outfile = fopen(logfile, "a");
    if (outfile != NULL){
      fprintf(outfile, "New sedc file <%s>\n", sedcfname);
      fflush(outfile);
      fclose(outfile);
    }

    processSEDCFile();
  } else {
    // if (strlen(logfile) > 0){
    //    outfile = fopen(logfile, "a");
	  // if (outfile != NULL){
    //    fprintf(outfile, "same date, no file change <%s>\n", sedcfname);
    //    fflush(outfile);
    //    fclose(outfile);
    //}
    //}
  }

  pthread_mutex_unlock(&cfg_lock);
  return 0;
}


static void cleanupset(gpointer key, gpointer value, gpointer user_data){
  struct fset *fs = (struct fset*) value;
  //FIXME: do we need to do anything with the metrics?
  ldms_destroy_set(fs->sd);
}

static void term(void)
{
  if (dirnamex) free(dirnamex);
  if (filebasename) free(filebasename);
  if (filetype) free(filetype);
  if (logfile) free(logfile);
  if (setshortname) free(setshortname);


  if (mf) pclose(mf);
  if (sedcf) pclose(sedcf);
  g_hash_table_destroy(compidmap);
  g_hash_table_foreach(setmap, cleanupset, NULL);
  g_hash_table_destroy(setmap);
}


static struct ldmsd_sampler sedc_plugin = {
	.base = {
		.name = "sedc",
		.term = term,
		.config = config,
		.usage = usage,
	},
	.get_set = get_set,
	.sample = sample,
};

struct ldmsd_plugin *get_plugin(ldmsd_msg_log_f pf)
{
	msglog = pf;
	return &sedc_plugin.base;
}
