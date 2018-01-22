# flowmonitor
This module allows you to monitorize the file system looking for changes and fires actions based on user handlers that are attached to the flow core.

## Events

Events could be: `created`, `modified`, `moved` and `deleted` for files and directories as well.

Another useful event is `quite` that will be fired once when an specific elapsed time has passed after last modification of a monitorized tree. 


## Handler Plugins

###  PelicanHandler for (.md) markdown files

This plugin handles with pelican projects with taks like:

1. updating the headers of a modified field each time you save it.

2. populate the content of a new file based on the likelihood from a template list.

Template files are ordinary `.md` files located under `template` or `templates` folder.

The plugin will try to match the best template file based on the path of the template file.

If we have a folder structure like this:

- `templates/article.md`
- `templates/book.md`
- `templates/blog/simple.md`
- `templates/blog/beauty.md`
- `templates/blog/book.sumary.md`


And we create a file for example in

`content/blog/new.book.md`

the plugin will select the `templates/blog/book.sumary.md` template as its has more words (tokens) in common with the new file (`blog` and `book`)

Note than Tokens are any piece of information in the relative path.

For instance taking `templates/blog/book.sumary.md` as an example, the yields tokens are: `blog`, `book`, `summary` and `md` for available matching information.


###  SyncHandler for pelican projects

Sync articles from 2 pelican projects that match some include and exclude patterns.

The recommended use is to create a subclass and set the config in constructor.

Example:

```
	class BlogSyncHandler(SyncHandler):

	    def __init__(self, path, remotes):
	        SyncHandler.__init__(self, path, remotes)

	        self.add_rule(INCLUDE, r'.*\.svg$')
	        self.add_rule(INCLUDE, r'.*\.py$')
	        self.add_rule(INCLUDE, r'.*\.md$', r'Tags:\s*.*\W+(test)\W+.*')
	        self.add_rule(INCLUDE, r'.*\.md$', r'Series:\s*.*\W+(vega)\W+.*')

	        self.add_rule(EXCLUDE, r'.*\.css$')  # an example :)

	        delete_missing = False

```



### DashboardHandler

Create a Dashboard of active and pending tasks exploring all markdown files.

```
	- [ ] buy the milk tomorrow
	+ [ ] writing this document today
	- [x] already done task
```



### BackupHandler

Create compressed backups of git repositories and upload to your MEGA account.

The plugin has a rotation policy that maintain:

- 1 file for each week day
- 1 file per each week of the year
- 1 file per each month of the year
- 1 file per year

```
	project-736712.git.rar
	project.d2.git.rar
	project.d1.git.rar
	project.d0.git.rar
	project.w2.git.rar
	project.w1.git.rar
	project.m1.git.rar
	project.y18.git.rar
```





### Command line Example:

The command line syntax is:

`$ flowmonitor.py [plugin1] [folder1] [plugin2] [folder2] [plugin3] [folder3] ... `

As an example:

`$ flowmonitor.py pelican . pytest . pelican ~/Documents/blog`


will monitorize folder `.` with a `pelican` and `pytest` handlers and `~/Documents/blog` with another `pelican` handler as well.

This also creates a config file `flow.yaml` that will be used next time if user doesn't specify any arguments.

## Dependences

`sudo pip install watchdog`

