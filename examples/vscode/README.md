# Instructions on how to run a debug session using vscode
To run a Go project in debug mode, you'll need to install Go and a debugger. Here are the steps:

1 - Install Go: You can download Go from the [official website](https://go.dev/doc/install). Follow the instructions for your specific operating system.

2 - Install a debugger: The most common debugger for Go is Delve. You can install it by running `go get github.com/go-delve/delve/cmd/dlv` in your terminal.

3 - Clone this project `git clone git@github.com:fsouza/fake-gcs-server.git`

4 - Navigate to the project folder and open vscode

5 - Inside vscode. Go to the extensions tab, on the search bar, type go, and install the [go extension](https://marketplace.visualstudio.com/items?itemName=golang.go). Alternatively you can Launch VS Code Quick Open (Ctrl+P), paste the following command, and press enter. `ext install golang.Go`

6 - Copy the `launch.json` file in this folder to your root directory, and put it inside vscode/launch.json

7 - Go to the debugger tab (Control+Shift+D). From the dropdown menu select `Launch` (should already be selected). Click on the green arrow to start the debug session. If the session started succesfully you will see in the debug console something like
```
Starting: [REDACTED]/go/bin/dlv dap --listen=127.0.0.1:39271 --log-dest=3 from [REDACTED]/workspace/fake-gcs-server
DAP server listening at: 127.0.0.1:39271
Type 'dlv help' for list of commands.
time=2024-03-08T11:17:28.591-07:00 level=INFO msg="server started at http://0.0.0.0:4443"
```

To verify it works, open a terminal and run the cmd `curl --insecure https://0.0.0.0:4443/storage/v1/b`. You should get this response
```
{"kind":"storage#buckets"}
```
# Other configurations
You can change your `launch.json` to launch the debug session with different config options. Here are some examples:

## Default config
```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch",
            "type": "go",
            "request": "launch",
            "mode": "debug",
            "program": "${workspaceFolder}",
            "args": ["-backend", "memory"]
        }
    ]
}
```

## Change port number
Instead of running on default port 4443, runs on port 5434. 
For example this cmd will work ` curl --insecure https://0.0.0.0:5434/storage/v1/b`

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch",
            "type": "go",
            "request": "launch",
            "mode": "debug",
            "program": "${workspaceFolder}",
            "args": ["-backend", "memory", "-port", "5434"]
        }
    ]
}
```

## Run in http
Allows to run in http. For example this cmd will work ` curl http://0.0.0.0:4443/storage/v1/b`
```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch",
            "type": "go",
            "request": "launch",
            "mode": "debug",
            "program": "${workspaceFolder}",
            "args": ["-backend", "memory", "-scheme", "http"]
        }
    ]
}
```
