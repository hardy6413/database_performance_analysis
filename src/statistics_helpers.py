
def calculate_mean_and_median(res):
    means, median = {}, {}
    for col in res.columns:
        means.update({col: res[col].mean()})
        median.update({col: res[col].median()})
    return means, median
