import json

import boto3
from flask import Flask, request, render_template_string

app = Flask(__name__)

# Example of all three credentials for STS or Cognito tokens.
# In a real environment, you'd fetch these from your token provider,
# environment variables, or another secure source:
REGION_NAME = "us-east-1"
FIREHOSE_STREAM_NAME = "PUT-S3-Y2wsC"

# Initialize the Firehose client with all three parameters
firehose_client = boto3.client(
    "firehose",
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN  # <--- Must include for temporary creds
)

# Simple HTML form
form_html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Checkout Form</title>
</head>
<body>
    <h1>Simple Checkout</h1>
    <form action="/checkout" method="POST">
        <label>Product ID:</label>
        <input type="text" name="productId" required><br><br>

        <label>Quantity:</label>
        <input type="number" name="quantity" min="1" required><br><br>

        <label>Buyer Name:</label>
        <input type="text" name="buyerName" required><br><br>

        <label>Shipping Address:</label>
        <input type="text" name="shippingAddress" required><br><br>

        <button type="submit">Place Order</button>
    </form>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    return form_html

@app.route('/checkout', methods=['POST'])
def checkout():
    product_id = request.form.get('productId')
    quantity = request.form.get('quantity')
    buyer_name = request.form.get('buyerName')
    shipping_address = request.form.get('shippingAddress')

    data = {
        "productId": product_id,
        "quantity": quantity,
        "buyerName": buyer_name,
        "shippingAddress": shipping_address
    }

    record_data = json.dumps(data)

    try:
        response = firehose_client.put_record(
            DeliveryStreamName=FIREHOSE_STREAM_NAME,
            Record={"Data": record_data.encode("utf-8")}
        )
        return f"Order placed successfully! Record ID: {response['RecordId']}"
    except Exception as e:
        return f"Error placing order: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True)
