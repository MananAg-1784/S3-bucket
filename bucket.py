
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os

import config
from timezone import change_timezone
from progress import ProgressPercentage

class Bucket():
    def __init__(self, aws_credentials = None):
        # Creating the client connection to the s3 bucket using boto3
        if not aws_credentials:
            aws_credentials = config.aws_credentials
        self.client = boto3.client(
            's3',
            aws_access_key_id= aws_credentials["access key"],
            aws_secret_access_key= aws_credentials["secret key"],
            region_name = aws_credentials["region"]
        )
        self.bucket = aws_credentials["bucket_name"]
        self.file_metadata = ["LastModified", "ContentLength", "ContentType", "ETag"]

        # Validating the Credentials
        if not self.check_credentials():
            print("NOTE : Credentials are not validated")
        else:
            print("Credentials and client successfully validated")
        print()
    
    def check_credentials(self) -> bool:
        '''
        Checks if the credentials for the client connection are correct or not

        :return : Boolean, True is the credentials are validated else False
        '''
        try:
            # Trying to list all the bucets in the AWS account
            total_buckets = self.client.list_buckets()
            print("Credentials are valid")
            return True
        
        # No credentials are given
        except NoCredentialsError:
            print("Credentials not available.")
        # credentials values are incorrect / not available
        except ClientError as e:
            # Check for specific error codes
            if e.response['Error']['Code'] == 'InvalidAccessKeyId':
                print("The access key ID you provided does not exist in our records.")
            elif e.response['Error']['Code'] == 'SignatureDoesNotMatch':
                print("The request signature we calculated does not match the signature you provided.")
            else:
                print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return False

    def get_file_metadata(self, file_name:str) -> dict|bool:
        '''
        Checks if the object is present in the bucket or not

        :param file_name/object_name : Name of the object to get
        :return : Returns False if the file_name is not present,
                  else returns a dict containing some metadata of the file
                  (LastModified, ContentLength, ContentType, ETag)
        '''
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=file_name)
            response = {key : response[key] for key in self.file_metadata}
            response["LastModified"] = change_timezone(response["LastModified"])
            return response
        except Exception as e:
            if e.response['Error']['Code'] == '404':
                print("Object / File_name specified not found")
            else:
                print("Cannot get object details : ",str(e))
            return False

    def upload_file(self, file_path:str, file_name:str = None, replace:bool = False) -> bool :
        '''
        Upload a file to an S3 bucket

        :param file_path: Absolute path of the file to upload
        :param file_name: (Optional) File name in the S3 bucket, default file_name used
        :param replace: (Optional) If True replaces the file if it is already present, False then does not replace

        :return: True if file was uploaded, else False
        '''

        # If file_name was not specified, use default name from the location
        try:
            if file_name is None:
                file_name = os.path.basename(file_path)
            size = os.path.getsize(file_path)
            print(f"File name : {file_name}\nFile Size : {size} bytes")
        except FileNotFoundError:
            print("File cannot be found on the given location")
            return False

        # Check if the File_name is present in the S3 bucket
        if self.get_file_metadata(file_name):
            print("\nA file with the same file_name already exists")
            if not replace:
                return False

        try:
            # Uploding the file
            response = self.client.upload_file(
                file_path, self.bucket, file_name,
                Callback=ProgressPercentage(file_path)
                )
            print("\nFile uploaded....")
        except Exception as e:
            print("\nFile cannot be uplaoded\n....",e)
            return False
        print()
        return True
    
    def get_object_list(self, folder_name:str = None, check_folder:bool = False) -> list|bool:
        '''
        Find the list of files inside a particular folder if present
        Handles pagination also if there are more than 1000 objects in the folder

        :param folder_name: (Optinal) Folder name, ending with '\\' otherwise returns objects inside the S3 bucket itself
        :param check_folder: (Optional) Boolean, to check if folder is present or not otherwise returns list of objects inside the folder
        :return : If check_folder = True, Returns True if folder present
                  Returns List of all the details about the files present in the folder
                  Details about each object : (Key, LastModified, ContentLength, ContentType, ETag)
        '''
        try:
            object_list = []
            continuation_token = None
            while True:
                response = None
                # Forming the request parameters based on the function parameters 
                request_params = {'Bucket': self.bucket}   
                if folder_name:
                    folder_name = folder_name if folder_name.endswith('/') else folder_name + '/'
                    request_params["Prefix"] = folder_name
                if continuation_token:
                    request_params['ContinuationToken'] = continuation_token
                if check_folder:
                    request_params['MaxKeys'] = 2

                # Get list of objects in the bucket
                response = self.client.list_objects_v2(**request_params)

                # Returning wether the folder is present or not
                if check_folder:
                    if 'Contents' in response:
                        print("The folder is present in the bucket")
                        return len(response['Contents'])
                    else:
                        print("The folder is not present in the bucket")
                        return False

                if 'Contents' in response:
                    object_list.extend([obj for obj in response['Contents']])
                elif len(object_list) == 0:
                    print("No such folder found in the bucket.... ", folder_name)
                    return False
                
                # Pagination
                if response.get('IsTruncated'):
                    print("Continuation Token Present...")
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break
                    
            if not folder_name:
                print("Folder is present ..... \nTotal Files Inside: ", len(object_list))
                return object_list
            else:
                print("Folder is present ..... \nTotal files Inside : ", len(object_list)-1 if object_list[0]["Key"] == folder_name else len(object_list))
                return object_list[1:] if object_list[0]["Key"] == folder_name else object_list

        except Exception as e:
            print("Exception while getting files list in the bucket \n", e)
            return False

    def create_folder(self, folder_name:str, replace:bool=False) -> bool:
        '''
        Creates a new Folder in S3 bucket

        :param folder_name: Name of the Folder
        :param replace: (Optional) Forcefully creates a new folder erasing all the files inside the folder
        :return: True if folder was created, else False
        '''
        # Checks if the anme format is correct or not
        if not folder_name.endswith('/'):
            folder_name += '/'

        if self.get_object_list(folder_name, True):
            print("Folder already present, ", folder_name)
            if not replace:
                return True
        try:
            response = self.client.put_object(Bucket=self.bucket, Key=folder_name)
            print(f"\nFolder created.... {folder_name}")
        except Exception as e:
            print("Cannot create the folder...\n",e)
            return False
        return True

    def delete_object(self, object_name:str) -> bool:
        '''
        Delete a file/folder from the S3 bucket, IF the file/folder  is not present does not return any error
        Deletes the folder only if the folder does not have any data

        :param object_name: File name to be deleted, Folder name should end with a '/'
        :return : Boolean - True if deleted else False
        '''
        try:
            if object_name.endswith('/'):
                resp = self.get_object_list(object_name, True)
                if not resp:
                    return False
                elif resp > 1:
                    print("The Folder contains files ...\nUse Delete folder data and then the folder")
                    return False
            # Delete the original file
            self.client.delete_object(Bucket=self.bucket, Key=object_name)
            print(f"{object_name} => deleted successfully")
            return True
        except Exception as e:
            print("Cannot Delete the File : ", e)
            return False

    def move_file(self, old_name:str, new_name:str) -> bool:
        '''
        Moves / Rename the old_name to the new_name,by replacing the the file and deleting the older file 

        :param old_name : Older file key of the file to be renamed
        :param new_name : name to be replaced 
        :return : Boolean, True if renamed else False
        '''
        try:
            # Copy the file to the new key
            self.client.copy_object(Bucket=self.bucket, CopySource={'Bucket': self.bucket, 'Key': old_name}, Key=new_name)
            # Deletes the old file
            self.delete_object(old_name)
            print(f"{old_name} is successfully moved to {new_name}")
            return True
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("File name specified does not exists")
            print("Cannot rename the File : ",e)
            return False

    def get_file_link(self, file_name:str, expiration:int=3600) -> str|bool:
        '''
        Generate a pre-signed URL for accessing a private file in an S3 bucket.

        :param file_key: The key (path/name) of the file
        :param expiration: Time in seconds for the URL to remain valid (default is 1 hour)
        :return: The pre-signed URL or False
        '''
        try:
            if not self.get_file_metadata(file_name):
                raise Exception("File does not exists")

            response = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': file_name},
                ExpiresIn=expiration)
            return response
        except Exception as e:
            print("Cannot get the URl for the file.... ",e)
            return False

    def delete_folder_data(self, folder_name:str) -> bool:
        '''
        Deletes all the files and data inside a folder

        :param folder_name: Folder name to delete the files, Folder names ends with '/'
        :return : Boolean, True if delete else False
        '''
        try:
            folder_name = folder_name if folder_name.endswith('/') else folder_name + '/'
            objects = self.get_object_list(folder_name)
            if not objects:
                print("Either the Folder is empty or does not exists")
                return False
            
            # Delete each object
            objects_to_delete = [{'Key': obj['Key']} for obj in objects]
            self.client.delete_objects(Bucket=self.bucket, Delete={'Objects': objects_to_delete})
            print(f"\nFolder '{folder_name}' and its contents have been deleted.")
            return True

        except Exception as e:
            print("Cannot delete files inside the Folder : ",e)
            return False

    def move_folder(self, old_name:str, new_name:str) -> bool:
        '''
        Moves / Renames the old folder to new folder new, by transfering all the data to other folder also

        :param old_name : Folder name that is to changed
        :param new_name : New name or location of the folder
        '''
        old_name = old_name if old_name.endswith('/') else old_name + '/'
        new_name = new_name if new_name.endswith('/') else new_name + '/'

        try:
            # Creating the new Folder
            if not self.create_folder(new_name):
                raise Exception("New folder cannot be created")

            objects = self.get_object_list(old_name)
            for obj in objects:
                old_key = obj['Key']
                new_key = old_key.replace(old_name, new_name, 1)
                self.move_file(old_key, new_key)

            # Deleting the new folder
            if not self.delete_object(old_name):
                raise Exception("Cannot delete the old folder")

        except Exception as e:
            print("Cannot move folder : ", e)
            return False

# bucket_object = Bucket()
