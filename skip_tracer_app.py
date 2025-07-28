# Elliott Wagner Solutions - AI Skip Tracer
# Real Data Integration Using TruthFinder Profile URLs

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Elliott Wagner Skip Tracer", page_icon="📍")
st.title("📍 Elliott Wagner Solutions: AI Skip Tracer")
st.markdown("Rejuvenating & Restoring | Find Heirs, Owners, and Executors")

st.info("Upload a spreadsheet with the columns: First Name, Last Name, City, State")
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file)

        required_cols = ["First Name", "Last Name", "City", "State"]
        if not all(col in df.columns for col in required_cols):
            st.error(f"Excel must contain columns: {', '.join(required_cols)}")
        else:
            results = []
            for _, row in df.iterrows():
                first = row['First Name']
                last = row['Last Name']
                city = row['City']
                state = row['State']
                tf_link = f"https://www.truthfinder.com/results/?firstName={first}&lastName={last}&city={city}&state={state}"

                results.append({
                    "Full Name": f"{first} {last}",
                    "City": city,
                    "State": state,
                    "TruthFinder Search Link": tf_link
                })

            result_df = pd.DataFrame(results)
            st.success("✅ Links generated!")
            st.dataframe(result_df)

            st.download_button(
                label="📥 Download TruthFinder Links as Excel",
                data=result_df.to_excel(index=False, engine='openpyxl'),
                file_name="truthfinder_links.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    except Exception as e:
        st.error(f"❌ Error processing file: {e}")
else:
    st.warning("👆 Upload an Excel sheet to get started.")
