import argparse

from best_practice_etl.io.readers import S3Reader
from best_practice_etl.io.writers import S3Writer
from best_practice_etl.jobs.process import ProcessJob


def main():
    # region get arguments

    parser = argparse.ArgumentParser()
    parser.add_argument('--inventory-bucket', default='spot-data-bpc-prod', help='S3 bucket to read from')
    parser.add_argument('--bpc-bucket', default='processed', help='S3 bucket to write')
    parser.add_argument('--inventory-data-path', default='spot-data-bpc-prod', help='S3 path to read from')
    parser.add_argument('--bpc-data-set-path', default='processed', help='S3 path to write')
    parser.add_argument('--organization-id', default='', help='Organization ID')
    parser.add_argument('--account-id', default='', help='Account ID')
    parser.add_argument('--snapshot-id', default='all', help='Snapshot ID')
    parser.add_argument('--assessment-id', default='all', help='Assessment ID')

    parsed_args = parser.parse_known_args()[0]

    # endregion

    # region initialize

    reader = S3Reader(bucket=parsed_args.inventory_bucket, path=parsed_args.inventory_data_path)
    writer = S3Writer(bucket=parsed_args.bpc_bucket, path=parsed_args.bpc_data_set_path)

    # endregion

    # region Run Job

    job = ProcessJob(f'Process best practice ETL [assessment_id={parsed_args.assessment_id}]', reader, writer)
    job.execute(
        organization_id=parsed_args.organization_id,
        account_id=parsed_args.account_id,
        snapshot_id=parsed_args.snapshot_id,
        assessment_id=parsed_args.assessment_id)

    # endregion


if __name__ == '__main__':
    main()
