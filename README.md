# flowmonitor
This module allows you to monitorize the file system looking for changes and fires actions based on user handlers that are attached to the flow core.

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

For instance taking `templates/blog/book.sumary.md` as an example, the yields tokens are: `blog`, `book`, `summary` and `md` as available matching information.



### Command line Example:

  $ flowmonitor.py pelican . pytest . pelican ~/Documents/blog


Will monitorize . with a pelican handler and pytest handler and ~/Documents/blog with another pelican handler as well.
