# Pre-Bronze Workflow: Data Cleaning & SDV Synthesis

## Assumptions
- This review evaluates the two Jupyter Notebooks (`Project_CSV_Clean_For_SDV_Colab.ipynb` and `Project_Colab_NB.ipynb`) used for raw Kaggle data cleaning and SDV synthetic data generation.
- The code is judged against standard enterprise data engineering and MLOps practices. While notebook exploration is normal, code intended to prepare models and generate production-grade test data is held to a high standard of reproducibility and robustness.

## Section 1: Step-by-Step Implementation (with code)

- Step 1: Text Normalization and Column Pruning
  The pipeline begins by loading a messy CSV using `pandas` and `latin-1` encoding. It uses the `unicodedata` library to strip accents from strings (e.g., converting "São Paulo" to "Sao Paulo") and drops a hardcoded list of 30+ columns deemed unnecessary or sensitive.

```python
import pandas as pd
import unicodedata

def clean_text(text):
    if pd.isna(text): return ""
    text = str(text)
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

df = pd.read_csv('DataCoSupplyChainDataset.csv', encoding='latin-1')
df.columns = df.columns.str.strip()

text_cols = df.select_dtypes(include=['object']).columns
for col in text_cols:
    df[col] = df[col].apply(clean_text)

cols_to_drop = ['Customer Email', 'Customer Fname', 'Order Id', 'Latitude', 'Longitude'] # Truncated
existing_cols = [c for c in cols_to_drop if c in df.columns]
df_clean = df.drop(columns=existing_cols)
df_clean.to_csv('DataCo_Cleaned_Plain_English.csv', index=False, encoding='utf-8-sig')
```

- Step 2: Temporal Repair and Parsing
  Switching libraries from Pandas to `polars`, the next script reads a raw CSV (from a hardcoded Windows `D:\` drive path), sanitizes date strings, and attempts to parse multiple datetime formats using `coalesce`. It arbitrarily fixes years before 1900 by adding 2000, and recalculates the day of the week.

```python
import polars as pl
import os

folder_path = r"D:\Data Lake\very_raw"
full_path = os.path.join(folder_path, "supply_chain_sample_new_2.csv")
df = pl.read_csv(full_path)

df = df.with_columns(
    date_str_clean=pl.col("order date (DateOrders)").str.replace_all("/", "-").str.replace_all("  ", " ").str.strip_chars()
)

df_final = df.with_columns(
    parsed_date=pl.coalesce(
        pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %H:%M", strict=False),
        pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %I:%M:%S %p", strict=False),
        pl.col("date_str_clean").str.to_datetime("%m-%d-%y %H:%M", strict=False)
    )
).with_columns(
    Order_Year=pl.col("parsed_date").dt.year(),
    Order_Month=pl.col("parsed_date").dt.month(),
    Order_Day=pl.col("parsed_date").dt.day()
)

df_final = df_final.with_columns(
    Order_Year=pl.when(pl.col("Order_Year") < 1900).then(pl.col("Order_Year") + 2000).otherwise(pl.col("Order_Year"))
)

# ... Recalculates DayOfWeek and writes to CSV ...
```

- Step 3: Column Name Standardization
  The script re-reads the CSV, iterates through the column names, lowercases them, replaces spaces and brackets with underscores, strips trailing underscores, and saves the final "model-ready" CSV.

```python
# ... Polars read_csv logic ...
new_columns = []
for col in df2.columns:
    clean_name = col.strip().lower()
    clean_name = clean_name.replace(" ", "_").replace("(", "_").replace(")", "_")
    while "__" in clean_name: clean_name = clean_name.replace("__", "_")
    clean_name = clean_name.strip("_")
    new_columns.append(clean_name)

df2.columns = new_columns
df2.write_csv(output_path, separator=",")
```

- Step 4: SDV Metadata Definition and Model Training
  Moving to Google Colab, the environment mounts Google Drive. It reads the cleaned CSV back into Pandas, uses SDV's `SingleTableMetadata` to infer column types, applies manual overrides to force categorical types, and trains a CTGAN model (WGAN-GP architecture) for 500 epochs.

```python
import torch
import pandas as pd
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

df_trans = pd.read_csv('/content/drive/MyDrive/DataCo_Synthetic/fixed_columns_final.csv')

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df_trans)

metadata.update_column(column_name='customer_state', sdtype='categorical')
# ... more categorical updates ...

synthesizer = CTGANSynthesizer(
    metadata, epochs=500, batch_size=500, verbose=True, cuda=True
)
synthesizer.fit(df_trans)
synthesizer.save('/content/drive/MyDrive/DataCo_Synthetic/CTGAN_WGAN_ModelEpochs500.pkl')
```

- Step 5: Chunked Generation with Post-Hoc Constraints
  To generate 2 million rows without running out of memory, the script generates data in chunks of 100,000. Because the GAN often hallucinates invalid categorical combinations (e.g., placing California in the European market), the script forces "truth" by dropping GAN-generated columns and applying `inner` joins against unique tables derived from the real data.

```python
valid_order_geo = df_real[["order_state", "order_country", "order_region", "market"]].drop_duplicates()
# ... builds valid_products and valid_customer_geo ...

synthetic_batches = []
for i in range(total_chunks):
    batch = synthesizer.sample(num_rows=CHUNK_SIZE)

    # Enforce valid order geography
    batch = batch.drop(columns=["order_country", "order_region", "market"])
    batch = batch.merge(valid_order_geo, on="order_state", how="inner")

    # ... applies merges for products and customer geo ...

    synthetic_batches.append(batch)

df_final = pd.concat(synthetic_batches, ignore_index=True)
df_final = df_final.sample(n=NUM_ROWS_TO_GENERATE, random_state=42)
df_final.to_csv(OUTPUT_FILENAME, index=False)
```

## Section 2: Detailed Review
1. Overall architecture assessment: The architecture is highly fragmented. It relies on a sequence of disjointed Jupyter notebook cells executed across entirely different environments (local Windows machine -> Google Drive -> Colab). State is passed via intermediary CSV files, which is fragile.
2. Performance considerations: The data cleaning pipeline arbitrarily bounces between Pandas (for text parsing) and Polars (for date parsing/column renaming). This means the data is serialized and deserialized to disk multiple times, destroying any memory/speed efficiencies. The SDV model is appropriately configured to leverage CUDA.
3. Security implications: Mounting a personal Google Drive directly into a Colab instance (`/content/drive/MyDrive/...`) exposes the entire Drive filesystem to the notebook runtime. If this notebook were shared or compromised, significant data exfiltration could occur.
4. Maintainability evaluation: Notebooks are inherently difficult to maintain, version control, and test. Hardcoded local paths (`D:\Data Lake\very_raw`) mean this code cannot be run by another developer without manual editing. The sequence of execution relies entirely on the developer running cells in the correct order.
5. Testability assessment: Zero. There are no unit tests, no assertion checks, and no way to validate the `clean_text` logic or date parsing without running the full script and manually inspecting the CSV outputs.
6. Dependency analysis: The environment requires `pandas`, `polars`, `sdv`, and `torch`. The lack of a `requirements.txt` or `environment.yml` for the Colab environment means future library updates to `sdv` could permanently break the synthetic generation logic.

## Section 3: Flaws Analysis
1. **Flaw: Hardcoded, Environment-Specific Paths**
   - **What it is:** Code contains paths like `D:\Data Lake\very_raw` and `/content/drive/MyDrive/...`.
   - **Why it's a flaw:** It violates the principle of environmental portability. If another developer clones this repo, the code immediately crashes.
   - **Impact:** Complete failure to run in CI/CD, Docker containers, or on team members' machines.
   - **Suggested Fix:** Use standard `pathlib` for relative pathing, or rely on configuration files/environment variables (like the `config.py` used in the Bronze/Silver pipelines) to abstract the data lake location.

2. **Flaw: Arbitrary Mixing of Pandas and Polars**
   - **What it is:** Step 1 uses Pandas to clean text. It saves a CSV. Step 2 loads the CSV with Polars to fix dates. It saves a CSV. Step 3 loads the CSV with Polars to fix column names.
   - **Why it's a flaw:** IO operations (reading/writing disk) are the slowest part of data processing. Forcing three full disk round-trips when one in-memory pipeline could do the job is massively inefficient.
   - **Impact:** Inflated execution time and unnecessary wear on storage drives.
   - **Suggested Fix:** Consolidate the entire cleanup logic into a single, unified Polars pipeline script. Polars can easily handle text normalization (`str.replace_all`), date parsing, and column renaming in one pass.

3. **Flaw: Post-Generation Inner Join Constraints**
   - **What it is:** The CTGAN generates a synthetic state, region, and country. The script drops the GAN's region and country, and does an `inner` join against a real lookup table based *only* on the state.
   - **Why it's a flaw:** This completely destroys the statistical distributions the GAN just spent 500 epochs learning. If the GAN generated "Texas" 5,000 times but the real lookup table has 3 different rows for Texas (e.g., due to data entry errors like "Texas - US", "Texas - USA"), the inner join will massively multiply those rows. Conversely, if a generated state doesn't match the lookup exactly, those rows are silently deleted.
   - **Impact:** The resulting 2 million rows are no longer statistically representative of the GAN's learned distribution; they are heavily skewed by the join explosion.
   - **Suggested Fix:** Do not drop GAN columns to force an inner join. Instead, enforce geographical constraints *during* the SDV metadata definition using SDV's native "Conditional Sampling" or hierarchical modeling features.

4. **Flaw: Silent Date Coalesce Failure**
   - **What it is:** The date parsing uses `pl.coalesce` with `strict=False`.
   - **Why it's a flaw:** If a date string is so malformed it doesn't match any of the three provided formats, `strict=False` returns a `null`. The script continues without warning.
   - **Impact:** Downstream models receive null dates for orders, potentially breaking time-series validations or creating synthetic data with missing temporal data.
   - **Suggested Fix:** Add a post-parse validation check: `.filter(pl.col("parsed_date").is_null())`. If the count is > 0, throw an error or log the specific unparseable strings for manual review.

5. **Flaw: "Magic" Year Correction Logic**
   - **What it is:** `pl.when(pl.col("Order_Year") < 1900).then(pl.col("Order_Year") + 2000)`
   - **Why it's a flaw:** Adding 2000 is an assumption. If a date was parsed as year `0015`, it becomes `2015`. But if a typo caused the year to be `0215`, it becomes `2215` (a future date).
   - **Impact:** Silent data corruption. The model might train on orders placed in the 23rd century.
   - **Suggested Fix:** Define a strict acceptable range (e.g., `2010 to 2025`). If the corrected year falls outside this window, flag the row as invalid and drop it, rather than blindly adding integers.

6. **Flaw: Loop Mutation of DataFrame Columns**
   - **What it is:** The text cleanup loops over columns: `for col in text_cols: df[col] = df[col].apply(clean_text)`
   - **Why it's a flaw:** Mutating a Pandas dataframe in a loop is highly un-pythonic and performs poorly compared to vectorized operations.
   - **Impact:** Slow execution on large datasets.
   - **Suggested Fix:** Use vectorized string operations or apply the function across the subset at once: `df[text_cols] = df[text_cols].applymap(clean_text)`.

7. **Flaw: Non-Deterministic Sampling**
   - **What it is:** `df_final.sample(n=NUM_ROWS_TO_GENERATE, random_state=42)` is used to trim the data down to exactly 2 million rows.
   - **Why it's a flaw:** Because the chunking loop uses an `inner` join that drops and duplicates rows unpredictably, the dataframe going into the `sample` function will be of a completely unpredictable size.
   - **Impact:** If the inner joins delete too many rows, the dataframe will have fewer than 2 million rows, causing the `sample` function to crash with a `ValueError` (cannot sample more than population).
   - **Suggested Fix:** Generate data in a `while` loop, appending to a list until the total length exceeds 2 million, then truncate the list exactly. Do not rely on unpredictable inner joins.

## Brutal Review
What an absolute mess. This isn't data engineering; it's a series of disconnected, panic-driven hacks taped together inside Jupyter Notebooks.

Let's address the elephant in the room: The "Pre-Bronze" architecture. You are exporting a CSV from Pandas, loading it into Polars in the very next block, saving it again, and loading it into Polars *again* to rename columns. Are you actively trying to wear out your SSD? This entire multi-step file-hopping dance should be a single, elegant script.

Then we get to the Colab notebook. Hardcoding your personal `D:\` drive and your Google Drive paths directly into the code guarantees nobody else on earth can run this without breaking it. Have you ever heard of relative paths? Or environment variables?

But the absolute worst offense is the "Safety Constraints" logic at the end of the SDV generation. You train a highly sophisticated WGAN-GP model for 500 epochs to learn complex probability distributions, and then what do you do? You rip out the generated geographical columns and do a blind `inner` join against a raw truth table. You realize an inner join with duplicate keys multiplies data, right? If your truth table has multiple entries for a state, you are exponentially duplicating synthetic data. If the GAN hallucinates a bad state, the inner join drops the row entirely. By the time that loop finishes, your 2 million rows are a statistically skewed, butchered Frankenstein dataset that completely invalidates the purpose of using a GAN in the first place.

**Would I approve this PR?**
**No.** Not in a million years.
**Justification:** The code is unportable, un-testable, insanely inefficient with disk I/O, and the post-generation joins actively destroy the statistical integrity of the synthetic data model. This needs to be completely rewritten into a single Python script for cleaning, and the SDV constraints must be handled natively within the SDV metadata configuration, not via hacky Pandas merges.