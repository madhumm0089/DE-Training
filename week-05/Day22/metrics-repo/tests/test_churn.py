# from metrics_repo.analyzers.churn_analyzer import calculate_churn_rate
from metrics_repo.analyzers.churn_analyzer import calculate_churn_rate
from metrics_repo.analyzers.sales_analyzer import sales_sum


import pandas as pd


def test_churn_rate():

    df = pd.DataFrame({'status': ['active', 'churned', 'active', 'churned']})

    assert calculate_churn_rate(df) == 0.5
    

def test_sales_sum():

    a, b = 5, 5
    assert sales_sum(a, b) == 10

