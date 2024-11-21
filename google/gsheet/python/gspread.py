import os.path
import gspread

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
RANGE = "Class Data!A2:E"

def main():
# Autenticación
    path_creds = os.path.join(os.getcwd(), "google/gsheet/credentials.json")
    # gc = gspread.service_account(filename=path_creds)
    gc = gspread.service_account(filename=path_creds)
    sh = gc.open(SPREADSHEET_ID)
    print(sh.sheet1.get('A1'))  

if __name__ == "__main__":
  main()
