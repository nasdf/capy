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
                "create_Film": {
                        "_link": "bafyrgqeuxoslzbveygkuedqmhuhmwm7bofp6a26xrfrci7462ooz4opzpf6hzzbeerfvptif2s7hfi4z2hfmrq5ly64mjfcqx525vdhmerygq",
                        "title": "Beetlejuice",
                        "actors": [
                                {
                                        "_link": "bafyrgqfvjhyhccoqthft5saxamdjiykxxiis3wwqohztknbd23opg4daw2av77iwg2ktcfcsorr4wen6romcihzvpnwumk25hbat4vsfsbyak",
                                        "name": "Michael Keaton"
                                },
                                {
                                        "_link": "bafyrgqarwacdf5cchfbep7j54ofiv6j2pkp2luatpte5ot33yr2hygwf6jdh5zuxvmnbtivjtbfpm5mi3pxdzwx523hy6bolwk6cp2em53dy4",
                                        "name": "Winona Ryder"
                                }
                        ],
                        "director": {
                                "_link": "bafyrgqgjc3ltkkn7rqtw5dkwhcewce7qo5536i6jnw7kk7svlrtwfuwy3f7fx2j5ma2cacgggmnf6uszgib73j3kivbw5sultano237e7kyto",
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
