import tkinter as tk
from tkinter.filedialog import askopenfilenames


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.grid()
        self.configure(background="black")
        self.master.resizable(0, 0)

        self.frame = tk.Frame(self, width=640, height=480)
        self.frame.grid()

        self.tit


def main():
    app = Application()
    app.mainloop()

if __name__ == '__main__':
    main()