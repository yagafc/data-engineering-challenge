import argparse
import os
import dask.dataframe as dd

def uncategorized_count(group):
    """Count the number of 'Unknown' values in the group"""
    return (group == 'Unknown').sum()


def unique_domains(group):
    """Count the number of unique domains in the group"""
    return len(group.unique())


def ad_based_count(group):
    """Count the number of 'True' values in the 'is_ad_based' column"""
    return (group == True).sum()


def home_page_count(group):
    """Count the number of 'True' values in the 'is_home_page' column"""
    return (group == True).sum()


def calculate_base_metrics(grouped):
    """Calculate basic aggregated metrics by country and category"""
    return grouped.agg(
        total_domains=('domain', 'count'),
        subsections_count=('subsections', 'sum'),
        home_page_count=('is_home_page', 'sum'),
        ad_based_count=('is_ad_based', 'sum')
    ).reset_index()


def aggregate_by_country(df):
    """Aggregate the number of domains by country"""
    return df.groupby('country').agg(total_country_domains=('domain', 'count')).compute().reset_index()


def aggregate_by_category(df):
    """Aggregate the number of domains by category"""
    return df.groupby('category').agg(total_category_domains=('domain', 'count')).compute().reset_index()


def calculate_percentage_metrics(metrics, total_domains_column, total_column_name, percentage_column_name):
    """Calculate percentage metrics for the given metrics"""
    metrics[percentage_column_name] = ((metrics[total_domains_column] / metrics[total_column_name]) * 100).round(2)
    return metrics


def calculate_ad_based_and_home_page_percentages(metrics):
    """Calculate the percentage of ad-based and home page domains in the metrics"""
    metrics['ad_based_percentage'] = ((metrics['ad_based_count'] / metrics['total_domains']) * 100).round(2)
    metrics['home_page_percentage'] = ((metrics['home_page_count'] / metrics['total_domains']) * 100).round(2)
    return metrics


def calculate_metrics_by_country_category(df):
    """Calculate extended aggregate metrics by country and category by calling the appropriate functions"""
    grouped = df.compute().groupby(['country', 'category'])

    metrics = calculate_base_metrics(grouped)

    country_domains = aggregate_by_country(df)
    merged = metrics.merge(country_domains, how='left', on=['country'])

    category_domains = aggregate_by_category(df)
    merged = merged.merge(category_domains, how='left', on=['category'])

    merged = calculate_percentage_metrics(merged,
                                          'total_domains',
                                          'total_country_domains',
                                          'country_domain_percentage')
    merged = calculate_percentage_metrics(merged,
                                          'total_domains',
                                          'total_category_domains',
                                          'category_domain_percentage')

    merged = calculate_ad_based_and_home_page_percentages(merged)

    metrics_ddf = dd.from_pandas(merged)
    return metrics_ddf


def load_data(file_path):
    """Load the DataFrame from Parquet files into Dask."""
    return dd.read_parquet(file_path)


def save_to_arrow(df, partitions, output_path):
    """Save the DataFrame to Arrow format (using Parquet) with partitioning by specified columns"""
    df.to_parquet(output_path, engine='pyarrow', partition_on=partitions)


def parse_args():
    parser = argparse.ArgumentParser(description="Calculate metrics")
    parser.add_argument('--source', default='../../data/output/',
                        help='Path to the source file (default is set)')
    parser.add_argument('--destination', default='../../data/metrics/', help='Directory to store metrics')
    return parser.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.destination):
        os.makedirs(args.destination)

    ddf = load_data(args.source)
    metrics_ddf = calculate_metrics_by_country_category(ddf)
    save_to_arrow(metrics_ddf, ['country', 'category'], args.destination)
    print("Metrics calculation completed!")

if __name__ == "__main__":
    main()
