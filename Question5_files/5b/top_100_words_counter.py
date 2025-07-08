# Read the input file
with open("5a-output", "r", encoding="utf-8") as f:
    lines = [line.strip().split("\t") for line in f]

# Filter out invalid lines and single-character words
filtered_lines = [(word, int(freq)) for word, freq in lines if len(word) > 2 and freq.isdigit()]

# Sort by frequency in descending order and take the top 100
sorted_words = sorted(filtered_lines, key=lambda x: x[1], reverse=True)[:100]

# Write the top 100 words to a new file
with open("top_100_words.txt", "w", encoding="utf-8") as f:
    for word, freq in sorted_words:
        f.write(f"{word}\t{freq}\n")

print("Top 100 words saved in top_100_words.txt")

