from ConfigParser import ConfigParser

def config(filename = "databaseSetup/database.ini", section="postgresql"):
    """Extract the config informations.

    Parameters
    ----------
    filename : The name of the configuration file from which we want to extract the informations (default is 'databaseSetup/database.ini')
    section : The section from which we want to search (default is 'postgresql')

    Raises
    ------
    Exception
        If the given section couldn't be found in the given filename

    Returns
    -------
    dictionnary(str, str)
        A dictionnary with the config informations found in the section of the file
    """
    parser = ConfigParser()
    parser.read(filename)

    if parser.has_section(section):
        db = {}
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        return db
    else:
        raise Exception("Section {0} not found in the {1} file, couldn't config the database.".format(section, filename))
