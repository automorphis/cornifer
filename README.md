# Cornifer

Cornifer is an easy-to-use data manager for experimental mathematics. 

# How to get

Make sure you have Python and `pip` installed and from a terminal, run

`pip install cornifer`

If you are using SageMath on Windows, you cannot use Cornifer if you are running SageMath from Cygwin. You may either run SageMath
from a Linux distro inside a virtualbox, or use WSL2 to run SageMath.

# How to use

I am writing a tutorial which you can find [here](https://github.com/automorphis/cornifer/tree/master/docs/tutorial.pdf). That
PDF is obviously still a work in progress, but I figured it was worth including rather than not. Later on in the PDF,
there are slideshows that work in Abode Reader and Chrome, although not in Firefox. You may also read the docstrings
on the various classes and methods.

# Requirements

Cornifer requires Python version 3.5 or greater.

## System requirements

Cornifer works on Windows and Linux. I have not yet tested macOS. It does not work in Cygwin.

# How to develop

From a terminal, run

`git clone https://github.com/automorphis/cornifer.git`

To test Cornifer, run

`python -m unittest discover ./tests`

in the top-level project directory.


# Namesake

This project is named after the [helpfully humming navigational bug of Hallownest.](https://hollowknight.fandom.com/wiki/Cornifer)