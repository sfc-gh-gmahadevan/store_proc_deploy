import os
import zipfile

def create_zip(directory, zip_name):
    # Create a ZipFile object
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Iterate through all the files in the directory
        for root, _, files in os.walk(directory):
            for file in files:
                # Create the complete file path
                file_path = os.path.join(root, file)
                # Add file to the zip file, using relative path
                zipf.write(file_path, os.path.relpath(file_path, directory))

if __name__ == "__main__":
    directory_to_zip = "/Users/gmahadevan/workspace/snowpark_projects/store_proc_deploy/NonPII/SilverRaw_synapse/DimLoader"  # Replace with your directory path
    zip_file_name = "script.zip"  # Name of the output ZIP file
    create_zip(directory_to_zip, zip_file_name)
    print(f"Created ZIP file: {zip_file_name}")
