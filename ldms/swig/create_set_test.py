#######################################################################
# -*- c-basic-offset: 8 -*-
# Copyright (c) 2015 National Technology & Engineering Solutions
# of Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.
# Copyright (c) 2015 Open Grid Computing, Inc. All rights reserved.
#
# This software is available to you under a choice of one of two
# licenses.  You may choose to be licensed under the terms of the GNU
# General Public License (GPL) Version 2, available from the file
# COPYING in the main directory of this source tree, or the BSD-type
# license below:
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#      Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#      Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials provided
#      with the distribution.
#
#      Neither the name of Sandia nor the names of any contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
#      Neither the name of Open Grid Computing nor the names of any
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#      Modified source versions must be plainly marked as such, and
#      must not be misrepresented as being the original software.
#
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#######################################################################


'''
Created on May 4, 2015

'''

import sys
from argparse import ArgumentParser
from ovis_ldms import ldms
import pytest
import traceback


"""Example of how to use ldms swig interface to create a set

Most APIs are the same as the C APIs and are documented in
the OVIS/ldms/src/core/ldms.h file. The swig-specific APIs could
be found in ldms.i located at OVIS/ldms/swig.
"""

def create_ldms_set_instance():
    """Create an ldms instance instance

    A instance instance can be created exactly the same as the C code.
    You could look at the example in the create_metric_set() function of
    the meminfo sampler plugin.
    """
    schema_name = "example_schema"
    instance_name = "example_instance"

    metric_names = ["metric_a", "metric_b", "metric_c"]
    metric_types = [ldms.LDMS_V_U64, ldms.LDMS_V_S32, ldms.LDMS_V_F32]

    print "Creating schema '{0}'".format(schema_name)
    schema = ldms.ldms_schema_new(schema_name)

    print "Adding metrics"
    for mname, mtype in zip(metric_names, metric_types):
	mindex = ldms.ldms_schema_metric_add(schema, mname, mtype)
	print "    Adding {0} of type {1}".format(mname, mtype)
	if mindex < 0:
	    raise Exception("Failed to create metric {0}".format(mname))

    print "Creating set '{0}'".format(instance_name)
    instance = ldms.ldms_set_new(instance_name, schema)
    if instance is None:
	raise Exception("Failed to create the instance {0}".format(instance_name))

    print "Setting user data"
    for metric_index in range(0, len(metric_names)):
	instance.metric_user_data_set(metric_index, 10000 + metric_index)

    print "Setting producer name to 'localhost'"
    instance.producer_name_set("localhost")

    #===================
    # You could destroy the schema.
    #===================
    ldms.ldms_schema_delete(schema)

    return instance

def update_metric_values(instance):
    """Update metric values in a set instance

    It is equivalent to the sample() function in ldmsd sampler plug-ins.

    @param instance: a set instance
    """
    print "--------"
    print "Begin a transaction"
    ldms.ldms_transaction_begin(instance)

    #===================
    # Set/update the value of metrics a and b
    #
    # ldms_metric_set_<value type>(set instance, metric index, value)
    #
    # The metric index is the order of added metrics to the schema, starting from 0.
    #===================
    print "Setting metric values"
    instance.metric_value_set(0, 123)
    instance.metric_value_set(1, -5)
    instance.metric_value_set(2, 6.789)

    print "End the transaction"
    print "--------"
    ldms.ldms_transaction_end(instance)

def get_metric_values(instance):
    """Print the metric values in the set instance

    @param instance: A set instance
    """

    #===================
    # Get the number of metrics in the instance
    #===================
    num_metrics = ldms.ldms_set_card_get(instance)

    print "{0: <10}{1: <15}{2: <15}{3: <15}".format("name", "type",
	    "value", "user data")

    for i in range(0, num_metrics):
	#===================
	# Get the metric name
	#===================
	mname = instance.metric_name_get(i)

	#===================
	# Get the metric type as string
	#===================
	mtype = instance.metric_type_as_str(i)

	#===================
	# Get the metric value according to type.
	#===================
	mvalue = instance.metric_value_get(i)

	#===================
	# Get the metric value according to type.
	#===================
	mudata = instance.metric_user_data_get(i)

	print "{0: <10}{1: <15}{2: <15}{3: <15}".format(mname, mtype,
		mvalue, mudata)

DEFAULT_PRE_ALLOC_MEM = 1024

def main():
    try:
	parser = ArgumentParser()
	parser.add_argument('--mem', help = "Pre-allocated Memory",
			    default = DEFAULT_PRE_ALLOC_MEM)
	args = parser.parse_args()

	#===================
	# This will pre-allocate memory to create metrics and set instances.
	#===================
	ldms.ldms_init(args.mem)   # This is mandatory call when using libldms.

	instance = create_ldms_set_instance()
	update_metric_values(instance)

	#===================
	# Print set meta data and data
	#
	# See more in pytest.py
	#===================
	pytest.show_meta(instance)

	get_metric_values(instance)

	#===================
	# Delete the set instance
	#===================
	ldms.ldms_set_delete(instance)

	return 0
    except:
	traceback.print_exc()


if __name__ == '__main__':
    sys.exit(main())
