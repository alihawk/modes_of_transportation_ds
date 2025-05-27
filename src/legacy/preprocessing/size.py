from common_imports import *

def list_files_and_sizes(folder_path):
    folder = Path(folder_path)
    if not folder.exists():
        print(f"Folder {folder_path} does not exist.")
        return
    
    for file in folder.iterdir():
        if file.is_file():
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"{file.name}: {size_mb:.2f} MB")

if __name__ == "__main__":
    folder = "data"
    list_files_and_sizes(folder)