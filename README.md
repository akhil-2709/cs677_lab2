1) How to run the Program ?

Ensure sellers.txt file is empty
From cli, run this command:  python3 main.py -N 6

N - number of peers

2) Checking for leader election
Note down the trader_host and trader_port number from the logs
Run “sudo lsof -i :<trader_port>” and note the process_id of the trader from the output 
Then, run “kill -9 <process_id>” to kill the trader and then check the logs or terminal to verify that the leader election algorithm has been triggered.
