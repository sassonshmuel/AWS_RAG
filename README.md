# AI Engineer course - AWS RAG Project



## Demo Video

Watch the demo video:

[▶ Watch the demo](video/demo.mp4)

If the video does not open directly in GitHub, you can download it and watch locally.

## Usage
Run the project and open the browser on port 5000.<br/>
Upload files to S3.<br/>
On every upload/delete, S3 will send a message to SQS.<br/>
The program will poll SQS every minute and once it finds a message, it will start ingestion on Bedrock, and delete the message.<br/>
Once the ingestion starts, indications will appear on the UI.<br/>
Once the ingestion ends, the documents will be updated on the lower right corner.<br/>
