Title: A Dashboard
Subtitle: Overview here!
Date: $date$
Modified: $date$
Tags: kpi
Authors: $author$
Summary: KPI Dashboard
Lang: en
Series: Focus

Priority Tasks
==================================

{% for status, priority_tasks in all_tasks.group_by('priority', ['*'],
    once=True, exact=True, notmaching=False, skip_empty=False).items() -%}

{% for task in priority_tasks -%}
{{task.priority }} [{{task.done }}]: [{{task.text }}]({{task.url -}})
{% endfor %}
{% endfor %}

----

{% for status, done_tasks in all_tasks.group_by('done', [' '],
    once=True, exact=True, notmaching=True, skip_empty=False).items() -%}

{{translate_done(status)}} Tasks
==================================

{% for tag, tasks in done_tasks.group_by('tags', all_tags, once=False).items() %}

{{tag}}
---------------------
({{len(tasks)}} tasks)

{% for task in tasks -%}
{{task.priority }} [{{task.done }}]: [{{task.text }}]({{task.url -}})
{% endfor %}
{% endfor %}

----
{% endfor %}



