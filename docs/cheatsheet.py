# Loop over all saved data.
with reg.open() as reg:              # Opens the Register.
    for apri in reg:                 # Loops over all ApriInfo.
        for blk in reg.blks(apri):   # Loops over all Blocks with the given apri (blk is automatically opened).
            for datum in blk:        # Loops over the data of blk.
                # Do something with datum.

# Loop over every 10th Block.
with reg.open() as reg:                                              # Opens the Register.
    for apri in reg:                                                 # Loops over all ApriInfo.
        for i, (startn, length) in enumerate(reg.intervals(apri)):   # Loop over Block intervals with given apri (please see the docs
                                                                     # for enumerate https://docs.python.org/3/library/functions.html#enumerate).
            if i % 10 == 0:                                          # If this is the 10th Block...
                with reg.blk(apri, startn, length) as blk:           # ... then load it and open it.
                    # Do something with blk.

# DO NOT DO THIS!
# The function `reg.intervals()` runs significantly faster than `reg.blks()`, because `reg.blks()` will load
# every single Block, whereas `reg.intervals()` only loads the startn and length. We do not want to load every
# single Block, we want to load every 10th Block.
with reg.open() as reg:                           # Opens the Register.
    for apri in reg:                              # Loops over all ApriInfo.
        for i, blk in enumerate(reg.blks(apri)):  # Loop over, load, and open all Blocks with given ApriInfo.
            if i % 10 == 0:                       # If this is the 10th Block...
                # Do something with blk           # ... then do something with it.

# Test if the Register reg1 is a subregister of reg2.
with open_regs(reg1, reg2) as (reg1, reg2):    # Opens the Registers reg1 and reg2.
    if reg1 in reg2.subregs():                 # Tests if reg1 is in the list `reg2.subregs()`.
        # reg1 is a subregister of reg2
    else:
        # reg1 is not a subregister of reg2

# Load the 100th entry without loading the entire Block it belongs to.
# (Only works for NumpyRegister and its subclasses.)
with reg.open() as reg:                         # Opens the Register (reg must be a NumpyRegister).
    datum = reg.get(apri, 100, mmap_mode = "r") # Load the 100th entry. mmap_mode = "r" means "memory mapping mode
                                                # is readonly". Memory mapping makes it possible to load part
                                                # of a Block without loading the entire Block, which is sometimes
                                                # (but not always) more efficient.

# Add the Register reg1 as a subregister of reg2
with open_regs(reg1, reg2) as (reg1, reg2):    # Opens the Registers reg1 and reg2.
    reg2.add_subreg(reg1)                      # Add reg1 as a subregister as reg2.



