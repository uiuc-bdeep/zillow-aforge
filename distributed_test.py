import csv

file = "/gpfs01/hadoop/user/cng10/Erick/zillow/stores/ca_hedonics.csv"

list = []

def fibonacci(n):
    if(n <= 1):
        return n
    else:
        return(fibonacci(n-1) + fibonacci(n-2))
print(fibonacci(100))
