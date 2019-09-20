 # -*- coding: utf-8 -*-
"""
This python coding pushes all data from two files at the same time.

@author: A0206686W
"""

import threading
import os
from time import ctime

# Define two open functions to open two .py files
def open_File01(args):
    print("File01 begins at %s."%ctime())
    os.system(args)
    print("File01 finishes at %s."%ctime())
def open_File02(args):
    print("File02 begins at %s."%ctime())
    os.system(args)
    print("File02 finishes at %s."%ctime())
    
# Create an empty list
thread_list = []
# Create two threadings
t1 = threading.Thread(target=open_File01, args=('A0206686W_FD001.py',))
thread_list.append(t1)
t2 = threading.Thread(target=open_File02, args=('A0206686W_FD002.py',))
thread_list.append(t2)

# Start two threadings
for t in thread_list:
    t.setDaemon(True)
    t.start()

# Wait for two threading to end
t.join()

print("All data have been punished at %s!"%ctime())
    