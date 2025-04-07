import os
import dotenv
import supabase
import pandas as pd
import pyzipper
from datetime import datetime
import shutil
from zipfile import ZipFile
    

dotenv.load_dotenv()

class SafeDownloader:
    DEFAULT_OUTPUT_DIR = "downloaded_files"
    
    def __init__(self, output_dir=None):
        self.supabase = supabase.create_client(
            os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
        )
        self.bucket = self.supabase.storage.from_("test")
        self.output_dir = output_dir or self.DEFAULT_OUTPUT_DIR

    def get_all_records(self):
        records = self.supabase.table("records").select("*").execute()
        return records
    
    def generate_timeline(self, output_dir=None):
        output_dir = output_dir or self.output_dir
        os.makedirs(output_dir, exist_ok=True)
        records = self.get_all_records()
        df = pd.DataFrame(records.data)
        output_path = os.path.join(output_dir, "timeline.csv")
        df.to_csv(output_path, index=False)
        return output_path
    
    def download_files(self, output_dir=None):
        output_dir = output_dir or self.output_dir
        records = self.get_all_records()
        for record in records.data:
            if record['file_name']:
                file_name = record['file_name']
                try:
                    result = self.bucket.download(file_name)
                    output_path = os.path.join(output_dir, file_name)
                    with open(output_path, 'wb') as f:
                        f.write(result)
                except Exception as e:
                    print(f"Error downloading file {file_name}: {str(e)}")
        return output_dir
    
    def generate_zip_with_password(self, password: str, folder_to_zip=None):
        folder_to_zip = folder_to_zip or self.output_dir
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"secure_archive_{timestamp}.zip"
        
        with pyzipper.AESZipFile(zip_filename, 'w', compression=pyzipper.ZIP_LZMA, 
                                encryption=pyzipper.WZ_AES) as zipf:
            zipf.setpassword(password.encode('utf-8'))
            
            for root, _, files in os.walk(folder_to_zip):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=os.path.dirname(folder_to_zip))
                    zipf.write(file_path, arcname)
        
        return os.path.abspath(zip_filename)
    
    def embed_zip_in_docx(self, docx_template_path, zip_file_path, output_docx_path):
        temp_extract_path = "temp_docx_extract"
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path)
        os.makedirs(temp_extract_path)

        with ZipFile(docx_template_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)

        embedding_folder = os.path.join(temp_extract_path, "word", "embeddings")
        os.makedirs(embedding_folder, exist_ok=True)
        hidden_zip_name = "archive.zip"
        shutil.copy(zip_file_path, os.path.join(embedding_folder, hidden_zip_name))

        base_name = "embedded_output"
        shutil.make_archive(base_name, 'zip', temp_extract_path)
        shutil.move(base_name + ".zip", output_docx_path)

        shutil.rmtree(temp_extract_path)
        return output_docx_path
        

if __name__ == "__main__":
    safe_downloader = SafeDownloader()
    safe_downloader.generate_timeline()
    downloaded_path = safe_downloader.download_files()
    zip_path = safe_downloader.generate_zip_with_password("test_password")
    safe_downloader.embed_zip_in_docx(docx_template_path="Sample_Press_Release.docx", zip_file_path=zip_path, output_docx_path="Public_Press_Release.docx")
    