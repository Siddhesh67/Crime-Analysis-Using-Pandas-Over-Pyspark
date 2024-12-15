import os

file_path = "/Users/smitbhabal/Downloads/Cleaned_Crime_Chicago.csv"
if os.path.exists(file_path):
    print("File exists!")
else:
    print("File not found!")
