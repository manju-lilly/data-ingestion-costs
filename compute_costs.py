#!/usr/bin/env python3
import os
from aws_cost_calculator import AWSCalculator
import csv

def read_csv(path):
    """[summary]
    read csv file
    Args:
        path ([type]): [description]
    """
    with open(path, 'r') as f:
        lines = f.read().splitlines()    
        return lines

def process_batch_resource_cnt():
    ## lambda
    
    ## happy path :) only
    statemachine_transitions = 5
    num_parallel = 2  ## 50 cnt
    

def transform_resource_cnt():
    pass

def enrich_resource_cnt():
    pass

    


    

