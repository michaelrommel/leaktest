# Instructions

1. Create the file to send to the S3 bucket:

```
dd if=/dev/random of=mediumfile.dat bs=1048576 count=18
```

2. Set your environment variables to the S3 credentials, like AWS_ACCESS_KEY, AWS_SECRET_KEY, S3BUCKET

3. In one terminal run the receiver `receiver-tokiocopy`

4. In another terminal start the sender `sender`

In the receiver terminal a list of memory allocation values are printed in .csv format.

