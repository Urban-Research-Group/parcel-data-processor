# Mel Data Processing Work Progress

Overview of Process

Step 1. Converting digest, appeals, and sales files to standard form

Step 2. Processing clean digest, appeals, and sales files into separate multi-year file

Step 3. Running ownership key script on digest to identify unique owners within the county

Step 4. Run Yuqi's geocoding script on digest if parcel boundary file exists (all but cobb, clayton, and dekalb)

Then send off to other projects

## Clayton

| Step | Progress |
|--------------------------|----------------------------------------------|
| Convert digest, appeals, and sales | Digest completed locally by HUP member Daniel Fang; appeals and sales to come. Files can be found in Data Engineering channel or in `output/clayton/1-raw_converted` |
| Process into single multi-year files | Digest completed locally by HUP member Daniel Fang; appeals and sales to come. Files can be found in Data Engineering channel or in `output/clayton/``2-processed_clean` |
| Run ownership key script | Needs rerun ownership YZ |
| Run geocoding script | Will need to be geocoded thru Google or Mapbox - IP |

## Cobb

| Step | Progress |
|--------------------------|----------------------------------------------|
| Convert digest, appeals, and sales | Digest and Appeals completed; Sales to come |
| Process into single multi-year files | Digest and Appeals completed; Sales to come |
| Run ownership key script | Completed MJ/Verified by YZ |
| Run geocoding script | Will need to be geocoded thru Google or Mapbox - IP |

## DeKalb

| Step                                 | Progress |
|--------------------------------------|----------|
| Convert digest, appeals, and sales   | IP       |
| Process into single multi-year files |          |
| Run ownership key script             |          |
| Run geocoding script                 |          |

## Fulton

| Step                                 | Progress                     |
|--------------------------------------|------------------------------|
| Convert digest, appeals, and sales   | 3/3 Completed                |
| Process into single multi-year files | 3/3 Completed                |
| Run ownership key script             | Completed MJ/Verified by YZ |
| Run geocoding script                 | Completed YZ/Reviewed MJ     |

## Gwinnett

| Step | Progress |
|---------------------------|---------------------------------------------|
| Convert digest, appeals, and sales | 3/3 completed locally by HUP member Daniel Fang; appeals and sales to come. Files can be found in Data Engineering channel or in `output/gwinnett` |
| Process into single multi-year files | 3/3 completed locally by HUP member Daniel Fang; appeals and sales to come. Files can be found in Data Engineering channel or in `output/gwinnett/2-processed_clean` |
| Run ownership key script | Completed MJ/Verified by YZ |
| Run geocoding script | Completed YZ/Needs Review MJ |

## Paulding

| Step | Progress |
|---------------------------|---------------------------------------------|
| Convert digest, appeals, and sales | 3/3 Completed. Files can be found in Data Engineering channel or in `output/paulding/2-processed_clean` |
| Process into single multi-year files | 2/3 Completed: need to merge 2 digest files between wingap and tyler (diff data structure & columns) |
| Run ownership key script |  |
| Run geocoding script |  |
