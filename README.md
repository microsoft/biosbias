# biosbias

This will help recreate the dataset in the following paper:

Maria De-Arteaga, Alexey Romanov, Hanna Wallach, Jennifer Chayes, Christian Borgs, Alexandra Chouldechova, Sahin Geyik, Krishnaram Kenthapadi, Adam Kalai. Bias in Bios: A Case Study of Semantic Representation Bias in a High Stakes Setting. Proceedings of FAT*, 2019.

Note: requires python 3 and python packages: warcio (to process the common crawl), pebble (for multiprocessing with timeouts)

Just run ./recreate.sh and it will download the bios and put them in a python pickled file called BIOS.pkl. Note: the more cores you have on your machine the faster it will be. For example, on a machine with 64 cores, it might take about 6 hours per archive times 16 archives = 4 days. 

Further details:
* download_bios.py takes as an argument an arxiv number and downlaods and extracts the bios into a .pkl file starting with the corresponding CC path.
* preprocess.py merges all these bios and also creates a version of the bio with names and pronouns scraped.
* the result is a pickled list of bio records.
* each bio record is a dictionary 
* r["title"] tells you the noramlized title
* r["gender"] tells you the gender (binary for simplicity, determined from the pronouns)
* r["start_pos"] indicates the length of the first sentence. 
* r["raw"] has the entire bio
* So the classification task is to predict r["title"] from r["raw"][r["start_pos"]:]
* The field r["bio"] contains a scrubbed version of the bio (with the person's name and obvious gender words (like she/he removed)

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
