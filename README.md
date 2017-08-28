# flowmonitor
This module allows you to monitorize the file system looking for changes and fires actions based on user handlers that are attached to the flow core.

## Events

Events are `created`, `modified`, `moved` and `deleted` file or directories.

Another useful event is `quite` that will be fired once when an specific elapsed time has passed after last modification of the monitorized tree. This event can be setup for long term processes.


## Handler Plugings

###  Pelican for (.md) markdown files

1. Pelican will update the Modified field each time you save the file

2. Populate the content for a new file or empty file based on templates

The template files can be located in any folder under 'template' or 'templates' folder.

Template files are ordinary `.md` files

The plugin will try to match the best template file based on the name and path of the template file.

If we have a folder structure like this:

- `templates/article.md`
- `templates/book.md`
- `templates/blog/simple.md`
- `templates/blog/beauty.md`
- `templates/blog/book.sumary.md`


And we create a file for example in

`content/blog/new.book.md`

the plugin will select the `templates/blog/book.sumary.md` template as its has more tockens in common with the new file (`blog` and `book`)

Note than Tokens are any piece of information in the relative path

For instance taking `templates/blog/book.sumary.md` as an example, the yields tokens are: `blog`, `book`, `summary` and `md` for available matching information.

###  PyTest for (.py) test files

Not finished yet. The idea is to launch `py.test` when there is some time since any file is changed.



### Command line Example:

The command line syntax is:

`$ flowmonitor.py [plugin1] [folder1] [plugin2] [folder2] [plugin3] [folder3] ... `

As an example:

`$ flowmonitor.py pelican . pytest . pelican ~/Documents/blog`


will monitorize folder `.` with a `pelican` and `pytest` handlers and `~/Documents/blog` with another `pelican` handler as well.

## Dependences

`pip install watchdog`

