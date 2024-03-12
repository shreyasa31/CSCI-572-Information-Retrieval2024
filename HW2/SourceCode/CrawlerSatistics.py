import http
import pandas as pd

with open("C:/Users/HP/Desktop/MSCS/USC_CS/IR/fetch_latimes.csv", "r", encoding="UTF-8") as f:
    data = pd.read_csv(f, header=0)
    fetches_attempted = data.shape[0]
    fetches_succeeded = data[data["Status"] < 300].shape[0]
    fetches_failed = data[data["Status"] > 300].shape[0]
    status_codes = data.groupby(data["Status"]).count().to_dict()["URL"]

with open("C:/Users/HP/Desktop/MSCS/USC_CS/IR/visit_latimes.csv", "r", encoding="UTF-8") as f:
    data = pd.read_csv(f, header=0)
    # # Convert the 'Size' column to numeric, coercing errors to NaN
    # data["Size"] = pd.to_numeric(data["Size"], errors='coerce')
    #
    # total_urls_extracted = data["# of Outlinks"].sum()
    # less_1KB = data[data["Size"] < 1024].shape[0]
    # less_10KB = data[(1024 <= data["Size"]) & (data["Size"] < 10 * 1024)].shape[0]
    # less_100KB = data[(10 * 1024 <= data["Size"]) & (data["Size"] < 100 * 1024)].shape[0]
    # less_1mb = data[(100 * 1024 <= data["Size"]) & (data["Size"] < 1024 * 1024)].shape[0]
    # greater_1mb = data[1024 * 1024 <= data["Size"]].shape[0]
    # content_types = data.groupby(data["Content-Type"]).count().to_dict()["URL"]

    # Remove the 'Byte' part and convert the 'Size' column to numeric
    data['Size'] = data['Size'].str.replace(' Byte', '').astype(int)
    total_urls_extracted = data["# of Outlinks"].sum()
    # Categorize the file sizes
    less_1KB = data[data["Size"] < 1024].shape[0]
    less_10KB = data[(1024 <= data["Size"]) & (data["Size"] < 10 * 1024)].shape[0]
    less_100KB = data[(10 * 1024 <= data["Size"]) & (data["Size"] < 100 * 1024)].shape[0]
    less_1mb = data[(100 * 1024 <= data["Size"]) & (data["Size"] < 1024 * 1024)].shape[0]
    greater_1mb = data[data["Size"] >= 1024 * 1024].shape[0]
    content_types = data.groupby(data["Content-Type"]).count().to_dict()["URL"]

with open("C:/Users/HP/Desktop/MSCS/USC_CS/IR/urls_latimes.csv", "r", encoding="UTF-8") as f:
    # Specify column names if you know the structure of your CSV
    column_names = ['URL', 'Status']  # Adjust the column names based on your file's structure
    data = pd.read_csv(f, header=None, names=column_names)

    # Now you can process your data as needed
    unique_extracted = data.shape[0]
    unique_within = data[data["Status"] == "OK"].shape[0]
    unique_outside = data[data["Status"] == "N_OK"].shape[0]

with open("./CrawlReport_latimes.txt", "w") as f:
    f.write(f"Name: Shreya Sadashiva Kamath\n")
    f.write(f"USC ID: 7553148882\n")
    f.write(f"News site crawled: latimes.com\n")
    f.write(f"Number of threads: 16\n")
    f.write(f"\n")

    f.write(f"Fetch Statistics\n")
    f.write(f"================\n")
    f.write(f"fetches attempted: {fetches_attempted}\n")
    f.write(f"fetches succeeded: {fetches_succeeded}\n")
    f.write(f"fetches failed or aborted: {fetches_failed}\n")
    f.write(f"\n")

    f.write(f"Outgoing URLs:\n")
    f.write(f"==============\n")
    f.write(f"Total URLs extracted: {total_urls_extracted}\n")
    f.write(f"# unique URLs extracted: {unique_extracted}\n")
    f.write(f"# unique URLs within News Site: {unique_within}\n")
    f.write(f"# unique URLs outside News Site: {unique_outside}\n")
    f.write(f"\n")

    f.write(f"Status Codes:\n")
    f.write(f"=============\n")
    for code in sorted(status_codes.keys()):
        f.write(f"{code} {http.HTTPStatus(code).phrase}: {status_codes[code]}\n")
    f.write(f"\n")


    f.write(f"File Size:\n")
    f.write(f"=============\n")
    f.write(f"Files smaller than 1KB: {less_1KB}\n")
    f.write(f"Files between 1KB and less than 10KB: {less_10KB}\n")
    f.write(f"Files between 10KB and less than 100KB: {less_100KB}\n")
    f.write(f"Files between 100KB and less than 1MB: {less_1mb}\n")
    f.write(f"Files greater than or equal to 1MB: {greater_1mb}\n")
    f.write(f"\n")


    f.write(f"Content Types:\n")
    f.write(f"==============\n")
    for content in sorted(content_types.keys()):
        f.write(f"{content}: {content_types[content]}\n")