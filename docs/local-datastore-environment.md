# Local Datastore environment setup

To run the sample on a local installation of Google Cloud Datastore, or to use it for working with
the Datastore in your tests, please perform the following steps:

1. Download the [SDK](https://cloud.google.com/sdk/downloads) and install it. 
   The SDK contains `gcloud` utility. To install the SDK, run the `install` script from it.

2. Make sure that `gcloud` utility is accessible from the command line.

3. Run `gcloud init` to initialize the SDK.

4. Run `gcloud components install cloud-datastore-emulator` to install the
   Datastore emulator.

5. Start the emulator with `gcloud beta emulators datastore start`. Leave it
   running in this terminal.

6. In a separate terminal, run `gcloud beta emulators datastore env-init` to
   print the connection variables. It displays:

```
export DATASTORE_DATASET=spine
export DATASTORE_HOST=http://localhost:8081
export DATASTORE_EMULATOR_HOST=localhost:8081
export DATASTORE_PROJECT_ID=spine
```

#### See Also

* [Using the GCD tool](https://cloud.google.com/datastore/docs/tools/)
