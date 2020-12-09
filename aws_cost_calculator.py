#!/usr/bin/env python3

class AWSCalculator(object):
    """[summary]
    Compute costs
    Args:
        object ([type]): [description]
    """
    def __init__(self):
        self.free_tier = False
        return
    
    def lambda_pricing(self, number_of_executions, memory, execution_time):
        """[summary]
        Lambda function pricing - https://s3.amazonaws.com/lambda-tools/pricing-calculator.html#
        Args:
            number_of_executions ([type]): [description]
            memory ([type]): [description]
            execution_time ([type]): [description]
        """

        num_executions = number_of_executions
        allocated_memory = memory
        execution_time = execution_time

        executions_to_count = (num_executions - 1000000) if self.free_tier else num_executions
        requests_costs = 0
        if executions_to_count > 0:
            requests_costs = (executions_to_count / 1000000) * .20
        
        compute_seconds = num_executions * (execution_time / 1000)
        compute_gbs = compute_seconds * (allocated_memory / 1024)
        total_compute = (compute_gbs - 400000) if self.free_tier else compute_gbs

        execution_costs = 0
        if total_compute > 0:
            execution_costs = total_compute * 0.00001667
        
        total_costs = requests_costs + execution_costs

        return total_costs

    def statemachine_costs(self, number_of_executions, number_of_parallel, state_transitions):
        total_transitions_per_execution = number_of_parallel * (state_transitions * \
                                                                number_of_executions)

        executions_to_count = (total_transitions_per_execution - 4000) if self.free_tier else total_transitions_per_execution
            
        execution_costs = 0
        if executions_to_count > 0:
            execution_costs = execution_costs * 0.000025

        return execution_costs

    def textract_costs(self, total_pages_processed):
        """[summary]
        Estimate without freetier
        Args:
            total_pages_processed ([type]): [description]
        """
        total_price = 0.0015*total_pages_processed if total_pages_processed < 1000000 else (
                (1000000 * 0.0015) + ((total_pages_processed - 1000000) * 0.0006))
        
        return total_price

    

        
        


