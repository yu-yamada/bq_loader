from google.cloud import bigquery
import yaml
import slackweb
import datetime

# slaclのwebhook urlを設定
slack = slackweb.Slack(url="slack_webhook_url")
slack_error = slackweb.Slack(url="slac_webhook_url")


def main():

    f = open("load_list.yml", "r+")
    d = yaml.load(f)

    client = bigquery.Client(project=d['project'])

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1

    now = datetime.datetime.now()

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV

    for target in d['targets']:

        try:
            table_ref = client.dataset(target['dataset']).table(target['table_name'])
            # uriは環境に合わせて変更
            uri = f"gs://{d['project']}/{target['table_name']}/{now:%Y-%m-%d}/*.gz"
            load_job = client.load_table_from_uri(
                uri, table_ref, job_config=job_config
            )  # API request
            log(f"Starting table load {target['dataset']}.{target['table_name']}")

            load_job.result()  # Waits for table load to complete.
            log("Job finished.")

            destination_table = client.get_table(table_ref)
            log(f"Loaded {destination_table.num_rows} rows.")
            log(f"JobResult {load_job.error_result}")
        except Exception as e:
            error(f"Error:{e}")


def log(message):
    print(message)
    slack.notify(text=message)


def error(message):
    print(message)
    slack_error.notify(text=message)


if __name__ == "__main__":
    main()
