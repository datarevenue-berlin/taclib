======
taclib
======

Taclib stands for **Task as Containers Library**, and it was created to ease running
luigi tasks in Kubernetes.


Cluster Role
------------
Each luigi task will be running in a pod as a containerized application. The luigi controller
pod needs to have a permission to create new pods in the cluster, in order to do that we have
to create a Cluster Role and bind it to an active service account.
The resource file ``cluster_role.yaml`` contain the necessary roles which allow
the controller pod to create task pods.
To grant the controller this permission we need to apply this role to our cluster through
``kubectl apply -f cluster_role.yaml``


Example
-------
In the directory example there are two python scripts that will serve as an example
on how to use **taclib**.

::

    ._ example_task.py
    import luigi
    from taclib.task import KubernetesTask

    class ExampleTask(KubernetesTask):

        name = luigi.Parameter(default="taclib")

        @property
        def image(self):
            return "my-custom-image:0.1"

        @property
        def command(self):
            return ["python", "-m", "script.py", "--name", self.name]

        def output(self):
            return luigi.LocalTarget("_SUCCESS")


If you are familiar with Luigi, you will recognize the structure above.
We define a custom task that will inherit KubernetesTask. task will spawn a pod
on Kubernetes using the image you define either through the function ``image``
or through TACLIB_IMAGE environment variable, if none of them are defined taclib
will set the default image as `python:3-7-alpine`.

Once the pod is running, the task will fire the command defined in
``def command(self):``. Note that in the example the given command executes
``script.py``, i.e, this file should be in the image you create.


Taclib tasks can be either started from a luigi command on you local environment,
or right in the Kubernetes cluster using ``kubectl``.

::

    luigi --module example.example_task ExampleTask --local-scheduler

::

    kubectl run my-task \
        --restart Never \
        --image-pull-policy Always \
        --image my-image \
        --serviceaccount taclib-controller-sa
        -- luigi --module example.example_task ExampleTask \
            --scheduler-host my-luigi-scheduler \





