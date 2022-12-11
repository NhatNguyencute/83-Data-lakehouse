from Storage_layer import upload_file_csv_to_s3
from Processing_layer import ETL_csv_and_import_database


if __name__ == "__main__":
    #upload_file_csv_to_s3.upload_to_log_data_aws()
    upload_file_csv_to_s3.upload_to_song_data_aws()
    ETL_csv_and_import_database.ETL()



