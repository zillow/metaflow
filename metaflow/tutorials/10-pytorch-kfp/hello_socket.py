import socket
from time import sleep

from metaflow import FlowSpec, step, S3


def server_program():
    # get the hostname
    host = socket.gethostname()
    port = 5000  # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(20)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = conn.recv(1024).decode()
        if not data:
            # if data is not received break
            continue
        print("from connected user: " + str(data))
        conn.send("hi".encode())  # send data to the client
        if str(data) == "bye":
            print("bye!!")
            break

    print("exiting")
    conn.close()  # close the connection


def client_program(host, message="hello world"):
    # host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    while message.lower().strip() != 'bye':
        client_socket.send(message.encode())  # send message
        data = client_socket.recv(1024).decode()  # receive response

        print('Received from server: ' + data)  # show in terminal

        break

    client_socket.close()  # close the connection
    print("worked!")


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")
        self.ranks = [1, 2, 3, 4, 5, 6]
        self.next(self.hello, foreach="ranks")

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        my_rank = self.input
        print("my_rank", my_rank)
        if my_rank == 1:
            host = socket.gethostbyname(socket.gethostname())
            print("host", host)
            # master
            with S3(run=self) as s3:
                s3.put("rank_0_host", host)

            server_program()
        elif my_rank == 6:
            # sleep 200 seconds
            with S3(run=self) as s3:
                exists = False
                while not exists:
                    sleep(200)
                    print(".")
                    obj = s3.get("rank_0_host", return_missing=True)
                    exists = obj.exists
                host = obj.text

            print("host", host)
            client_program(host, message="bye")

        else:
            #
            with S3(run=self) as s3:
                exists = False
                while not exists:
                    sleep(1)
                    print(".")
                    obj = s3.get("rank_0_host", return_missing=True)
                    exists = obj.exists
                host = obj.text

            print("host", host)
            client_program(host)

        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == '__main__':
    HelloFlow()
