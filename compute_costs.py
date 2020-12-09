#!/usr/bin/env python3
import os
from aws_cost_calculator import AWSCalculator
import csv

data_path = "data/part-00000-e93e77cc-d3ab-4021-b840-43273581ac66-c000_dev (1).csv"

def read_csv(path):
    """[summary]
    read csv file
    Args:
        path ([type]): [description]
    """
    with open(path, 'r') as f:
        lines = f.read().splitlines()    
        return lines

def process_batch_cost(records):
    aws_calc = AWSCalculator()

    ## 1. load_parameters lambda
    ## lambda
    load_parameters_memory = 2240
    ## msec
    load_parameters_time = 900 * 1000

    ## 2. statemachine
    ## happy path :) only
    statemachine_transitions = 5
    num_parallel = 2  ## 50 cnt
    
    ## 3. process batch lambda
    process_batch_memory = 3008
    process_batch_time = 900 * 1000

    ## 4. job complete lambda
    job_complete_memory = 128
    job_complete_time = 900 * 1000
    
    ## data
    num_records = len(records)

    load_parameters_lambda_execution = 1
    statemachine_invocations = num_records // 100
    process_batch_lambda_execution = statemachine_invocations * 2 ## 2 times each statemachine invocation
    job_complete_lambda_execution = statemachine_invocations

    ## cost 
    cost = aws_calc.lambda_pricing(load_parameters_lambda_execution, load_parameters_memory, load_parameters_time) + aws_calc.statemachine_costs(statemachine_invocations, num_parallel, statemachine_transitions) + \
        aws_calc.lambda_pricing(process_batch_lambda_execution, process_batch_memory, process_batch_lambda_execution) + \
        aws_calc.lambda_pricing(
            job_complete_lambda_execution, job_complete_memory, job_complete_time)
    
    return cost

def transform_cost(records):
    aws_calc = AWSCalculator()

    # 1. extractPDF lambda
    extract_pdf_lambda_memory = 2240
    extract_pdf_lambda_timeout = 30 * 1000

    # 2. textract
    
    # 3. pdf to text lambda
    extract_pdftotxt_lambda_memory = 2240
    extract_pdftotxt_lambda_timeout = 30 * 1000

    csv_reader = csv.DictReader((line for line in records if not line.isspace(
    ) and not line.replace(",", "").isspace()), delimiter=',')

    csv_reader = list(csv_reader)
    num_pages = list(map(lambda x: int(x['num_pages']), csv_reader))
    num_records = len(csv_reader)
    
    cost = aws_calc.lambda_pricing(num_records, extract_pdf_lambda_memory, extract_pdf_lambda_timeout) + aws_calc.textract_costs(sum(num_pages)) + aws_calc.lambda_pricing(num_records, extract_pdftotxt_lambda_memory, extract_pdftotxt_lambda_timeout)

    return cost

def enrich_cost(records):
    aws_cost = AWSCalculator()
    num_records = len(records)

    # enrich invoke statemachie lambda
    enrich_invoke_statemachine_lambda_memory = 2240
    enrich_invoke_statemachine_lambda_time = 30 * 1000

    # enrich statemachine
    number_of_transitions = 7
    number_of_parallel = 1

    # negation lambda
    enrich_nd_lambda_memory = 2240
    enrich_nd_lambda_time = 30* 1000

    # sd lambda
    enrich_sd_lambda_memory = 2240
    enrich_sd_lambda_time = 30 * 1000
    
    # pfRA lambda
    enrich_pfRa_lambda_memory = 2240
    enrich_pfRa_lambda_time = 30 * 1000
    
    # jc lambda
    enrich_jc_lambda_memory = 2240
    enrich_jc_lambda_time = 30 * 1000

    print(aws_cost.lambda_pricing(num_records,
                                  enrich_invoke_statemachine_lambda_memory, enrich_invoke_statemachine_lambda_time))

    
    cost = aws_cost.lambda_pricing(num_records, enrich_invoke_statemachine_lambda_memory, enrich_invoke_statemachine_lambda_time) + aws_cost.statemachine_costs(num_records, number_of_parallel, number_of_transitions) + aws_cost.lambda_pricing(
        num_records, enrich_nd_lambda_memory, enrich_nd_lambda_time) + aws_cost.lambda_pricing(num_records, enrich_sd_lambda_memory, enrich_sd_lambda_time) + aws_cost.lambda_pricing(num_records, enrich_pfRa_lambda_memory, enrich_pfRa_lambda_time) + aws_cost.lambda_pricing(num_records, enrich_jc_lambda_memory, enrich_jc_lambda_time)

    return cost
    
def elastic_search_cost(records):
    aws_cost = AWSCalculator()
    num_records = len(records)
    memory = 128
    time = 30 * 1000
    num_records = len(records)

    cost = aws_cost.lambda_pricing(num_records,
                                   memory, time)

    return cost
    

if __name__ == "__main__":
    # read data
    ## read csv file - divide into chunks of 100 - invoke statemachine (each invocation 2 parallel paths)
    records = read_csv(data_path)
    print(len(records))
    # process batch costs
    pb_costs = process_batch_cost(records)
    
    ## transform costs
    trans_costs = transform_cost(records)

    # enrich costs
    en_costs = enrich_cost(records)

    # elastic costs
    es_cost = elastic_search_cost(records)

    print(pb_costs, trans_costs, en_costs, es_cost)
    print('Total cost', sum([pb_costs, trans_costs, en_costs, es_cost]))
    
    """
    number of records not found:5754
    total number of records:3280	

    """


    

