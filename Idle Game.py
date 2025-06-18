import tkinter as tk
import threading
import time
import os
from tkinter import simpledialog
from pynput import mouse, keyboard
import pyautogui

import queue


# Game variables
Value = 0
resources_per_second = 0
upgrade_cost = 10
upgrade_factor = 2
save_file_name = ""  # Initially empty
IDLE_TIME = 290

# Class to represent a Shop Item
class ShopItem:
    def __init__(self, name, cost, description):
        self.name = name
        self.cost = cost
        self.description = description
#        self.effect = effect  # Function to modify game variables (e.g., increasing Value)

    def buy(self):
        global ShareHolderValue
        # Assuming ShareHolderValue is 
        if ShareHolderValue >= self.cost:  # You may need to define ShareHolderValue
            ShareHolderValue -= self.cost
            indivBuffs[self.name]+=1
            return True
        else:
            return False


# Shop Items
shop_items = [
    ShopItem('Coffee', 10, 'Increases your click power by .025 per click '),
    ShopItem('Pizza Parties', 15, 'Increase your Value passively by .025 per minute')
]



# Queue for communication between the listener thread and the main thread
key_event_queue = queue.Queue()


last_active_time = time.time()

# Function to toggle "Always on top"
def toggle_always_on_top():
    if root.attributes("-topmost"):
        root.attributes("-topmost", 0)  # Remove always on top
    else:
        root.attributes("-topmost", 1)  # Set always on top

# Function to open the Shop window
def open_shop_window():
    shop_window = tk.Toplevel(root)  # Create a new top-level window
    shop_window.title("Shop")

    # Use grid layout to arrange the items
    row = 0
    global shop_items
    for item in shop_items:
        # Create a button for each buff
        buff_button = tk.Button(shop_window, text=f"Buy {item.name.capitalize()}", command=lambda key=item.name: buy_buff(key))
        #buff_button = tk.Button(shop_window, text=f"Buy {item.name.capitalize()}", command=lambda item=item: buy(item))
        buff_button.grid(row=row, column=0, pady=10, padx=10)

        # Create a label to show the current Value of the buff
        buff_Value_label = tk.Label(shop_window, text=f"Cost: {item.cost}")
        buff_Value_label.grid(row=row, column=1, pady=10, padx=10)

        # Create a description label for each buff
        buff_description_label = tk.Label(shop_window, text=f"{item.description}")
        buff_description_label.grid(row=row, column=2, pady=10, padx=10)

        row += 1  # Move to the next row
        
def scoreBonus():
    global stats, shop_items  # Declare these variables as global
    curValue = stats["Value"]
    stats["Value"] = curValue + 10
        
  # Function to handle buff purchases
def buy_buff(buff_key):
    global stats, shop_items  # Declare these variables as global
    # You can set a cost for the buffs and deduct it from the player's resources
    if stats["Value"] >= shop_items[0].cost:  # Example cost, you can make  dynamic based on the buff
        curValue = stats[buff_key]    
        stats[buff_key] = curValue  + 1
        curValue = stats["Value"]
        stats["Value"] = curValue - shop_items[0].cost  # Deduct the cost from the player's share holder Value
        update_display()  # Update the display after purchasing a buff
    else:
        #display_text.insert(tk.END, f"\nNot enough ShareHolder Value to buy {buff_key}!\n")
        #display_text.yview(tk.END)
        print("")
        
        
# Upgrade function
def upgrade():
    global resources_per_second, upgrade_cost, ShareHolderValue
    if ShareHolderValue >= upgrade_cost:
        ShareHolderValue -= upgrade_cost
        resources_per_second += 1
        upgrade_cost *= 2
        update_display()  # Update the display after the upgrade
    else:
       # display_text.insert(tk.END, "\nNot enough ShareHolder Value to upgrade!\n")
        #display_text.yview(tk.END)
        print("")

# Function to update the displayed text
def update_display():
    global stats_labels, stats
    for label in stats_labels:
        stats_labels[label].config(text=f"{label}: {stats[label]}")

def check_id():
    
    global last_active_time

    idle_duration = time.time() - last_active_time
    if idle_duration >= IDLE_TIME:
        # Open Notepad
        #subprocess.Popen('notepad.exe')
        #time.sleep(1)  # wait for notepad to open
        
        # Click near center of screen to focus Notepad
        screenWidth, screenHeight = bonus_button.winfo_rootx(),bonus_button.winfo_rooty()
        pyautogui.click(screenWidth, screenHeight)
        time.sleep(0.2)
        
# Function to update the displayed text
def update_stat_display(labelName, Value):
    global ShareHolderValue, stats_labels
    stats_labels[labelName].config(text=f"{labelName}: {Value}")
  

# Function to handle key presses (every keystroke adds a point)
def on_key_press(key):
    global ShareHolderValue, stats
    try:
        # Increment Value for every key press
        # ShareHolderValue += 1
        clickVal = stats["Value"]
        stats["Value"] = clickVal + 1
        key_event_queue.put("update") # Put an "update" event in the queue
        global last_active_time
        last_active_time = time.time()
    except AttributeError:
        # Handle special keys (e.g., Shift, Enter, etc.) without causing errors
        pass
    
# Function to handle key presses (every keystroke adds a point)
def on_mouse_move(key):
    global ShareHolderValue, stats
    try:
        # Increment Value for every key press
        # ShareHolderValue += 1
        clickVal = stats["Value"]
        stats["Value"] = clickVal + .001
        key_event_queue.put("update") # Put an "update" event in the queue
        global last_active_time
        last_active_time = time.time()
    except AttributeError:
        # Handle special keys (e.g., Shift, Enter, etc.) without causing errors
        pass
    

# Function to process key events from the queue
def process_key_events():
    global stats
    while not key_event_queue.empty():
        event = key_event_queue.get()
        if event == "update":
            update_stat_display("Value",stats["Value"])  # Safely update the display on the main thread
            
    check_id()
    root.after(100, process_key_events)  # Check for key events again after 100 ms

# Game loop function to update display every 10 seconds
def game_loop():
    update_display()  # Update the display every second

# Function to open settings window
def open_settings_window():
    settings_window = tk.Toplevel(root)  # Create a new top-level window
    settings_window.title("Settings")

    # Add a checkbox for "Always on Top"
    always_on_top_check = tk.Checkbutton(
        settings_window,
        text="Always on Top",
        variable=always_on_top_var,
        command=toggle_always_on_top
    )
    always_on_top_check.pack(pady=10)

    # Position the settings window above the main window
    main_x = root.winfo_rootx()
    main_y = root.winfo_rooty()
    settings_window.geometry(f"200x50+{main_x}+{main_y-50}")  # Position it 120px above the main window
    settings_window.grab_set()  # Make it modal (focus on this window until closed)

# Function to ask for save file name and handle file operations
def ask_for_save_file_name():
    global save_file_name, stats
    
    top = tk.Toplevel()
    top.withdraw()  # Hide the window
    top.attributes("-topmost", True)  # Make it stay on top

    save_file_name = simpledialog.askstring("Save File Name", "Enter the save file name/Player Name:",parent=top)

    if save_file_name:
        save_dir = "saves"
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)  # Create the "saves" directory if it doesn't exist

        save_path = os.path.join(save_dir, f"{save_file_name}.txt")

        # Check if file exists
        if os.path.exists(save_path):
            # Read the file contents into a list (each line as a list item)
            with open(save_path, "r") as file:
                for line in file:
                    # Strip whitespace and split on the first ' - '
                    parts = line.strip().split(" - ", 1)
                    if len(parts) == 2:
                        stats[parts[0]] = float(parts[1])
        else:
            # If file doesn't exist, create it and initialize resources to 0
            with open(save_path, "w") as file:
                for stat in stats:    
                    file.write(f"{stat} - {stats[stat]}\n")  # Save both Value and passive to the file

        update_display()  # Update the display after reading the save file
        file_name_label.config(text=f"File: {save_file_name}")  # Update the label to show the current file name
        
def is_click_inside_window(x, y):
    root_x = root.winfo_rootx()
    root_y = root.winfo_rooty()
    root_width = root.winfo_width()
    root_height = root.winfo_height()

    return root_x <= x <= root_x + root_width and root_y <= y <= root_y + root_height

# Function to save resources to the file
def save_to_file():
    global stats
    save_dir = "saves"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)  # Ensure the "saves" directory exists

    save_path = os.path.join(save_dir, f"{save_file_name}.txt")

    # Save current resources to the file, overwriting the existing file
    with open(save_path, "w") as file:
        for stat in stats:    
            file.write(f"{stat} - {stats[stat]}\n")   # Save Value on the first line, passive on the second
    #display_text.insert(tk.END, f"\nSaved {ShareHolderValue} ShareHolder Value to {save_file_name}.txt\n")
    #display_text.yview(tk.END)

def exit_game():
    print("Saving game before exiting...")
    save_to_file()  # <- Your custom save function
    root.destroy()  # Close the tkinter window and end the session


# Set up the main Tkinter window
root = tk.Tk()
root.title("Share Holder Value Idle Game")

# Set window size and position it just above the taskbar
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Estimate the taskbar height (e.g., 40px), adjust this if needed
taskbar_height = 40

# Position the bottom bar just above the taskbar
bar_height = 20  # Height of the bottom bar (taskbar-like)
root.geometry(f"{screen_width}x{bar_height}+0+{screen_height - taskbar_height - bar_height}")
root.configure(bg="black")  # Background color (taskbar-like)
root.attributes("-topmost", True)  # Ensure the bar stays on top
#root.attributes("-transparentcolor", "black")  # Make the black color transparent (optional)
root.overrideredirect(True)  # Remove window decorations (no border, no title bar)

# Example: Add a label to the bottom bar
label = tk.Label(root, text="ShareHolder Value", fg="white", bg="black", font=("Arial", 14))
label.pack(side="left", padx=10) 

# Create a label to show the save file name persistently
file_name_label = tk.Label(root, text="File: No file selected", font=("Arial", 10))
file_name_label.pack(side="left", padx=10)

stats_labels = {}
stats = {"Value": 0, "Coffee": 0, "Energy Drinks": 0, "Unpaid Interns": 0}

for stat, Value in stats.items():
    label = tk.Label(root, text=f"{stat}: {Value}", fg="white", bg="black", font=("Arial", 9))
    label.pack(side="left", padx=5)
    stats_labels[stat] = label
   
# Variable to track the "always on top" setting
always_on_top_var = tk.BooleanVar()
always_on_top_var.set(True)  # Default to "Always on top" enabled
root.attributes("-topmost", 1)  # Set the window always 1 top initially


exit_button = tk.Button(root, text="Save and Exit", command=exit_game)
exit_button.pack(side="right", padx=10)

button = tk.Button(root, text="Save Game", command=save_to_file)
button.pack(side="right", padx=10)

# Add a button to open the settings window
settings_button = tk.Button(root, text="Settings", command=open_settings_window)
settings_button.pack(side="right", padx=10)


shop_button = tk.Button(root, text="Shop", command=open_shop_window)
shop_button.pack(side="right", padx=10)


bonus_button = tk.Button(root, text="Layoff Bonus!", command=scoreBonus)
bonus_button.pack(side="right", padx=10)


# Ask for save file name on start
ask_for_save_file_name()

# Start the game loop in a separate thread to prevent freezing the UI
#game_thread = threading.Thread(target=game_loop, daemon=True)
#game_thread.start()

update_display()
  

def start_listeners():
    with mouse.Listener(on_move=on_mouse_move, on_click=on_mouse_move) as mouse_listener, \
         keyboard.Listener(on_press=on_key_press) as keyboard_listener:
        mouse_listener.join()
        keyboard_listener.join()

        

# Start global key listener in a separate thread
listener_thread = threading.Thread(target=start_listeners, daemon=True)
listener_thread.start()


# Start the Tkinter main loop
root.after(100, process_key_events)  # Start checking for key events
root.mainloop()
