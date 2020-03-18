import tkinter as tk
import os

root = tk.Tk()
root.title("Running Python Script")

T = tk.Text(root, height=2, width=30)
T.pack()
T.insert(tk.END, "ETL is developed in Python\nin With Oracle DB\n")

canvas1 = tk.Canvas(root, width = 600, height = 400)
canvas1.pack()

label1 = tk.Label(root, text='Running ETL...', font=("Arial Bold", 50))
canvas1.create_window(300, 100, window=label1)

def close_window ():
    root.destroy()

def run():
    os.system('ETL.py')

btn = tk.Button(root, text="Click Me", bg="black", fg="white",command = run)
#btn.grid(column=100, row=20)
btn.pack()

button = tk.Button(text = "Quit", command = close_window)
button.pack()


tk.mainloop()
root.mainloop()


