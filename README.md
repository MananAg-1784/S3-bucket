# S3_bucket
`Amazon Simple Storage Service` (Amazon S3) is an object storage service that offers scalability, data availability and performance.
<br><br>This package will help you perform tasks in your S3 bucket though the use of simple Python functions. It can be easily be implemented with any of the existing code allowing it to manage and access storage directly through the bucket. 
<br>It uses the `boto3` package - Amazons SDK for Python.

## How to Use

Fill up the AWS details in the `config.py` file, It is not mandatory to configure the file but this method is recommended. You can also pass the credentials while creating the `bucket` object.
<br>If no credentials are passed while creating the object it takes defualt credentials from the config file.

```bash
# config.py file
aws_credentials = {
    "access key" : "",
    "secret key" : "",
    "region" : "",
    "bucket_name" : ""
}
```

Import the package in your file `run.py`
<br>Create an object of the class `bucket` from `bucket.py`

```bash
# run.py
from S3_bucket.bucket import Bucket
from env import credentials   # Importing your AWS details locally
bucket_obj = Bucket()
# OR
bucket_obj2 = Bucket(credentials)
```

Some Examples of the functions
```bash
obj.create_folder("Music")
obj.upload_file(file_path = "__path__", file_name = "new_file.txt", replace = True)
obj.move_folder("Music", "Albums")
obj.get_object_list("Albums\", check_folder = True)
```
*NOTE : While creating a S3 bucket for your app make sure to check the `Block all public access` to protect your files from unauthorised access.* 

## Contact 

For any questions or feedback, please contact at `mananagarwal1784@gmail.com` <br>
Visit my [Website](https://manan-portfolio.ddns.net/) to check out my works
