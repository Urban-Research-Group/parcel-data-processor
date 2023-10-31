# Set Up Working Environment
## Downloads
- Download Git
- Download Anaconda (comes with the latest version of Python)
- Download VSCode or other prefered text-editor

## Set Up Git
1. Make a GitHub account. You can do this with or without your GaTech email.
2. Open Git Bash.
3. Run the following command. This command will prompt you to login to GitHub and save your credientials the next time you use ```git push``` (don't worry about it now).
```bash
git config --global credential.helper store
```


## Clone Repo
1. Clone this repository from [Github](https://github.com/Urban-Research-Group/ga-tax-assessment/) by typing the following command into Git Bash.  
Ensure you have navigated to the directory you want, e.g. I put my code in my Documents folder. Git Bash will tell you the current directory in yellow text before you are prompted to enter input. You can change directory with ```cd {name}```.
```bash
git clone https://github.com/Urban-Research-Group/ga-tax-assessment/
```
2. Open the repo in VSCode.  
You can do this by opening the VSCode application like any other, then selecting File -> Open Folder, and selecting this folder.
3. Navigate to the code branch titled your name.  
You can do this in VSCode by selecting the branch icon in the lower left of your screen (looks like a 'V' with a word next to it, should say 'main', indicating the current branch).  
Or, you can do this in Git Bash with the following command:
```bash
git checkout {branch-name}
```

## Open the 'start-example' Jupyter Notebook File
Jupyter Notebooks are a feature of Interactive Python (IPython) and come with Anaconda. The notebook file (.ipynb file) contains further instructions.
