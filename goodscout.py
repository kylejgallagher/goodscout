import pandas as pd

# Can replace with original file "for_unique_monthly_divID_jun5test.csv"

file = "scout71525"
df = pd.read_csv(f"{file}.csv")

# Convert dates if needed
df["date_created"] = pd.to_datetime(df["date_created"], errors='coerce')

# Sort for consistent grouping
df = df.sort_values(by=["resume_contact_id", "resume_id", "date_created"])

# Define salutation, keyword, and exclusion patterns
# Maybe put day kanji in brackets to solve issue?
salutation_pattern = r"Dear|様|Hello|Hi |さま|Sama"
keyword_pattern = r"面接面接|来社|当日|カジュアル|面談|interview|Monday|Tuesday|Wednesday|Thursday|Friday|\(月\)|\(火\)|\(水\)|\(木\)|\(金\)|\（月\）|\（火\）|\（水\）|\（木\）|\（金\）|本日|曜日|meeting|平日|時|来週|以降"
exclude_pattern = r"candidate|applicant|sir|madam|sender|候補者"

# Function to process each (job_application_id, resume_id) group
# This will look through messages until they become good apps, and then return the date/time from when they are considered to be GA
def process_group(group):
    group = group.reset_index(drop=True)
    
    total_messages = len(group)
    match_index = None
    
    for idx, row in group.iterrows():
        body = str(row['response']).lower()
        
        has_salutation = pd.notnull(body) and bool(pd.Series([body]).str.contains(salutation_pattern, case=False, regex=True).iloc[0])
        has_keyword = pd.notnull(body) and bool(pd.Series([body]).str.contains(keyword_pattern, case=False, regex=True).iloc[0])
        has_candidate = pd.notnull(body) and bool(pd.Series([body]).str.contains(exclude_pattern, case=False, regex=True).iloc[0])
        
        # change depending on if you want good app or just any messae sent by company:
        # add and has_keyword for good app filtering. leave out for all messages
        if has_salutation and has_keyword and not has_candidate:
            match_index = idx
            break

    if match_index is not None:
        matched_row = group.loc[match_index].copy()
        matched_row["messages_until_keyword"] = match_index + 1
        return matched_row
    else:
        return None  # No valid message found, exclude this group

# Apply the function to each group
grouped = df.groupby(["resume_contact_id", "resume_id"])
results = grouped.apply(process_group)

# Drop empty groups and reset index
final_df = results.dropna(how='any').reset_index(drop=True)
final_df["type"] = "Scout"

# switch between the two for date formatting
# final_df["date_created"] = final_df["date_created"].dt.strftime("%Y/%m/%d")
# final_df["date_created"] = pd.to_datetime(final_df["date_created"], format="%Y/%m/%d")

# added to exclude message for readability

# print(df.columns())

# final = final_df.drop(columns=["response", "employer"])
final = final_df.drop(columns=["employer", "response", "messages_until_keyword"])

# May need to save as UTF-8 for Zoho. Former filename "client_message_for_unique_divID.csv"
final.to_csv(f"{file}_filtered.csv", index=False)
print(final)
print(f"Total unique application/resume pairs: {len(final)}")




# SQL query. exclude data from the day you're making the query for. in the query put todays date
emp_response_direct_only = """
SELECT resume_contact_response.resume_contact_id, resume_contact_response.date_created, employer.employer_id, employer.employer_type, resume_contact_response.employer, division.division_id, resume_contact_response.response, resume_contact.resume_id
FROM resume_contact_response
LEFT JOIN resume_contact ON resume_contact_response.resume_contact_id = resume_contact.resume_contact_id

LEFT JOIN employer ON resume_contact.employer_id = employer.employer_id

LEFT JOIN(
    SELECT
        employer_id,
        division_id
    FROM
        employer_access
    LEFT JOIN crm_v2.quote_detail ON
        crm_v2.quote_detail.quote_detail_id = employer_access.quote_detail_id
    LEFT JOIN crm_v2.quote ON
        crm_v2.quote_detail.quote_id = crm_v2.quote.quote_id
    GROUP BY
        division_id
) division
on employer.employer_id = division.employer_id
WHERE
	resume_contact_response.employer = 0 AND
    resume_contact_response.date_created BETWEEN '2024-01-01' and '2025-07-11'; 
"""

with_JSID = """
SELECT resume_contact_response.resume_contact_id, resume_contact_response.date_created, employer.employer_id, employer.employer_type, resume_contact_response.employer, division.division_id, resume_contact_response.response, resume_contact.resume_id, resume_contact.job_seeker_id
FROM resume_contact_response
LEFT JOIN resume_contact ON resume_contact_response.resume_contact_id = resume_contact.resume_contact_id

LEFT JOIN employer ON resume_contact.employer_id = employer.employer_id

LEFT JOIN(
    SELECT
        employer_id,
        division_id
    FROM
        employer_access
    LEFT JOIN crm_v2.quote_detail ON
        crm_v2.quote_detail.quote_detail_id = employer_access.quote_detail_id
    LEFT JOIN crm_v2.quote ON
        crm_v2.quote_detail.quote_id = crm_v2.quote.quote_id
    GROUP BY
        division_id
) division
on employer.employer_id = division.employer_id
WHERE
	resume_contact_response.employer = 0 AND
    resume_contact_response.date_created BETWEEN '2024-01-01' and '2025-07-15'; 


"""
