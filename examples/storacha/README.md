# Capy Storacha example

This example shows how to export and backup your data to IPFS and Filecoing using Storacha.

## Steps

Run the main file to create and export some data:

```bash
$ go run ./main.go
```

The results of the mutation should print out:

```json
{
        "data": {
                "createFilm": {
                        "title": "Beetlejuice",
                        "actors": [
                                {
                                        "name": "Michael Keaton"
                                },
                                {
                                        "name": "Winona Ryder"
                                }
                        ],
                        "director": {
                                "name": "Tim Burton"
                        }
                }
        }
}
```

If everything worked as expected, a file containing the exported data should now exist in your directory: `export.car`

To upload to Storacha you'll need to create an account and install the CLI by following the guide [here](https://docs.storacha.network/w3cli/).

Once you have the CLI installed and authenticated you can upload your export with the following command:

```bash
$ w3 up --car export.car
```

> NOTE: the `--car` flag is required otherwise the data will be uploaded as a normal file.

Congrats! Your data is now backed up to IPFS and Filecoin!
