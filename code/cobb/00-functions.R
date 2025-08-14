
# Function to get relevant files based on specified years
get_relevant_files <- function(input_folder, keywords) {
  files <- list.files(input_folder, recursive = TRUE, full.names = TRUE)
  keywords_pattern <- paste(keywords, collapse = "|")
  
  relevant_files <- files[str_detect(files, keywords_pattern)]

  ## if .txt then it is the data, if .xlsx then its the dictionary
  relevant_files_df <- data.frame(
    year = str_extract(relevant_files, "\\d{4}"),
    dict_path = ifelse(str_detect(relevant_files, "\\.xlsx$"), relevant_files, NA),
    data_path = ifelse(str_detect(relevant_files, "\\.txt$"), relevant_files, NA)
  ) %>%
    group_by(year) %>%
    summarise(
      dict_path = first(na.omit(dict_path)),
      data_path = first(na.omit(data_path)),
      .groups = "drop"
    )
  
  ## drop where either dict or data is not available
  relevant_files_df <- relevant_files_df %>%
    filter(!is.na(data_path) & !is.na(dict_path))
  
  return(relevant_files_df)
}

# Function to clean the data dictionary
clean_cobb_dictionaries <- function(table) {
  table <- table %>% 
    clean_names() %>%
    filter(!is.na(start_position) & !is.na(seq)) %>%
    ## make sure that each column is the correct type
    mutate(type = case_when(
      startsWith(length_3, "C") ~ "c",
      grepl("^N[0-9]+$", length_3) ~ "i",
      grepl("^N[0-9]+,[0-9]+$", length_3) ~ "d",
      TRUE ~ ""
    ))
  
  ##validate that sum of the lengths is equal to the intended length of the line
  tryCatch({
    if (sum(table$length_6) != rev(table$end_position)[1]) {
      stop("Error: Sum of lengths does not match the intended total length of the line.")
    }
    message("Data dictionary successfully cleaned.")
    
  }, error = function(e) {
    message("Data dictionary cleaning failed: ", e$message)
  })
  
  return(table)
}

# Function to process lines from .txt file based on dictionary structure
process_lines <- function(txt_lines, dictionary) {
  txt_lines <- txt_lines[stri_length(txt_lines) > 1]
  
  start_locs <- as.integer(dictionary$start_position)
  end_locs <- as.integer(dictionary$end_position)
  expected_length <- max(end_locs, na.rm = TRUE)
  
  ## check expected lengths
  bad_lines <- which(stri_length(txt_lines) != expected_length)
  if (length(bad_lines) > 0) {
    stop(glue("Found {length(bad_lines)} lines with incorrect length. Expected: {expected_length}"))
  }
  
  ## break up segments 
  processed_lines <- map_chr(txt_lines, function(line) {
    segments <- map2_chr(start_locs, end_locs, ~ substring(line, .x, .y))
    paste0('"', trimws(segments), '"', collapse = ",")
  })
  message("Txt successfully processed.")
  
  return(processed_lines)
}

# Function to save processed data as CSV
processed_file_to_csv <- function(processed_lines, dictionary, output_folder, txt_path, dict_path) {
  output_dir <- paste0(output_folder, "/TAXDATA", year)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  
  txt_file_path <- file.path(output_dir, paste0(tools::file_path_sans_ext(basename(txt_path)), ".csv"))
  
  tryCatch({
    header <- paste0('"', paste(dictionary$database_field, collapse = '","'), '"')
    output_with_header <- c(header, processed_lines)
    
    writeLines(output_with_header, txt_file_path)
    
    message(glue("File successfully saved: {txt_file_path}"))
  }, error = function(e) {
    message(glue("Error saving file: {e$message}"))
  })
  
  dict_file_path <- file.path(output_dir, paste0(tools::file_path_sans_ext(basename(dict_path)), ".csv"))
  
  tryCatch({
    write_csv(dictionary, file = dict_file_path)
    
    message(glue("Dictionary successfully saved: {dict_file_path}"))
  }, error = function(e) {
    message(glue("Error saving file: {e$message}"))
  })
}
