function doGet() {
  // Open the Google Sheet
  const sheet = SpreadsheetApp.openById("12MTYf03CoI_wTuT5CEgsnBq4At9tCZ4Cr5esHruzg7E").getSheetByName("Channels");
  
  // Get all data
  const data = sheet.getDataRange().getValues();
  
  // Convert data to JSON
  const jsonData = JSON.stringify(data);

  return ContentService.createTextOutput(jsonData).setMimeType(ContentService.MimeType.JSON);
}