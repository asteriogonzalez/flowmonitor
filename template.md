Title: A Dashboard
Subtitle: Overview here!
Date: 2017-08-29 15:50
Modified: 2017-08-29 20:40
Tags: kpi
Authors: agp
Summary: KPI Dashboard
Lang: en
Series: Focus


Pending Tasks group by Tags
==================================

{% for tags, grp in pending_group.items() %}
{{tags}}
---------------
{% for task in grp %}
- [{{task.done}}]: {{task.text -}}
{% endfor %}

{% endfor %}


Done Tasks group by Tags
==================================

{% for tags, grp in done_group.items() %}
{{tags}}
---------------
{% for task in grp %}
- [{{task.done}}]: {{task.text -}}
{% endfor %}

{% endfor %}


----

All Pending Tasks
=================

{{ len(pending_task) }} Tasks

{% for task in pending_task %}
- [{{task.done}}]: {{task.text -}}
{% endfor %}


All Done Tasks
=================

{{ len(done_task) }} Tasks

{% for task in done_task %}
- [{{task.done}}]: {{task.text}} {{task.tags -}}
{% endfor %}


