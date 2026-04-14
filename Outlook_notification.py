
import win32com.client as win32
outlook = win32.Dispatch("Outlook.Application")
mail = outlook.CreateItem(0)
mail.To = "you@company.com"
mail.Subject = "Notebook run status"
mail.Body = "Completed successfully."
mail.Send()
