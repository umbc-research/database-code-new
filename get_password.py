#!/usr/home/mb17/database_work/backend/bin/python3

def read_password(path):
    try:
        with open(path, 'r') as file:
            password = file.read().strip()  # Stripping extra spaces/newlines
        return password
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()

def main():
    password = read_password("./.password")
    return password

if __name__ == "__main__":
    main()

