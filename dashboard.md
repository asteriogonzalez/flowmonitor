Title: A Dashboard
Subtitle: Overview here!
Date: 2017-08-29 15:50
Modified: 2017-08-29 20:40
Tags: kpi
Authors: agp
Summary: KPI Dashboard
Lang: en
Series: Focus



{% for status, done_tasks in all_tasks.group_by('done', [' '],
    once=True, exact=True, notmaching=True, skip_empty=False).items() -%}

{{translate_done(status)}} Tasks
==================================

{% for tag, tasks in done_tasks.group_by('tags', all_tags, once=True).items() %}

{{tag}}
---------------------
({{len(tasks)}} tasks)

{% for task in tasks -%}
- [{{task.done -}}]: [{{task.text -}}]({{task.url -}})
{% endfor %}
{% endfor %}

----
{% endfor %}



