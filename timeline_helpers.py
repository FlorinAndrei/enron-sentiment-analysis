import pandas as pd

scores = pd.read_csv("sa_results.csv", index_col=0)
scores["Date"] = scores["Date"].astype("datetime64[ns]")
tlf_to_email = pd.read_csv("tlf_to_email.csv", index_col=0)
all_emails = tlf_to_email["From"].to_list()


def generate_timeline(e):
    return (
        scores[scores.apply(lambda x: e in x["From"], axis=1)][["Date", "compound"]]
        .set_index("Date")
        .rename(columns={"compound": e})
    )
