Initial version of the runtime/development environment configuration scripts:

deploy.sh is the header script for toolchain, runtime, pg
toolchain deploys the toolchain (no postgres)
runtime deploys the runtime including postgres
pg configures and starts postgres
pswdless_ssh generates a key and distributes it to a list of servers so that the current host can ssh to the list of servers (without a password). No values need to be specified when asked by SSH, except for the user's password. (See: http://www.open-mpi.org/faq/?category=rsh)
scidb_pkgs.sh copies a set of SciDB .deb packages to a host and installs them (after removing all *scidb* packages first)

Example:
 > bash -x deploy.sh runtime host1 root               # repeat for host2-N
 > bash -x deploy.sh pg host1 root 10.0.20.0/24       # on coordinator only

 > 'make packages' on build machine to produce *.deb files to be fed to 'deploy.sh ... pkgs ...'  
 > bash -x deploy.sh pkgs host1 root packages/*.deb   # *-dbg-* cannot be installed presently, dont install *-dev-* # repeat for host2-N

 > root@coordinator> sudo adduser scidb

 + You must also set up passwordless ssh (or other authentication) from coordinator to scidb@other-cluster-hosts in order for scidb to function.
 + Also, you life as a developer will be easier if you also set it up from myDesktop to scdib@coordinator.
    ---detailed instructions on setting up (6) and (7) are at the end ---
    ---but you should do it at this point ---

--- other things that must be done, no scripts written ----------

 scidb@coordinator> edit /opt/scidb/12.10/etc/config.ini[.DB] # to specify DB, instances
 scidb@coordinator> sudo -u postgres opt/scidb/12.10/scidb.py init_syscat [/opt/scidb/12.10/etc/config.ini.DB]
 scidb@coordinator> /opt/scidb/12.10/scidb.py initall  DB [/opt/scidb/12.10/etc/config.ini.DB ]
 scidb@coordinator> /opt/scidb/12.10/scidb.py startall DB [/opt/scidb/12.10/etc/config.ini.DB ]
 scidb@coordinator> /opt/scidb/12.10/scidb.py stopall  DB [/opt/scidb/12.10/etc/config.ini.DB ]


===== passwordless ssh ========
To set up passwordless ssh FROM <origin-host> to <target-host-list> for <user> [with src on <src-tree-hosta> ]

# you can't actually do the first example, as its not allowed to set it up as root.  maybe we can figure out a
# workaround?
[wishful thinking example: from my desktop to all nodes, as root, (recommended to make the installs easier)
          <src-tree-host>=myDesktop, <origin-host>=myDesktop, <target-host-list>=cluster nodes ]
    me@src-tree-host> su root
    me@src-tree-host> bash -x pswdless_ssh.sh <target-host-list>

#[optional example 1: repeat the above as scidb to at least the coordinator,
#(recommended to ease all admin steps including the intra-cluster passwordless ssh setup:]

    me@src-tree-host> su scidb # any way to do this and line below in one line with sudo?
    me@src-tree-host> bash -x pswdless_ssh.sh <target-host-list>=coordinator
      # paswordless-ssh-for-dummies questions:
      #            what to do if it asks to overwrite the rsa file
      #            what should we use a passphrase, since we are all sharing the scidb account
      #            (or does it matter?)
      #            what if tigor sets it up on p4xen7 to honor him logging in as scidb
      #            but I set it up from sbe ... if I say overwrite the rsa file,
      #            does it lose the info needed for tigors login from his box to work?

      # after doing this step, test it:
      me@myDesktop> sudo -u scidb ssh scidb@target   # will ask for me@myDesktop's password
                                                     # once if sudo not done recently

#[REQUIRED example 2, required for any mpi operation, unless another authentication method is arranged:
#          from coordinator to other nodes in cluster, as scidb(for scidb to do mpi over cluster)
#          <user>=scidb <src-tree-host>=myDesktop <origin-host>=coordinator, <target-host-list>=non-coordinator-nodes
    me@src-tree-host> scp pswdless_ssh.sh <scidb>@<origin-host>:/tmp
    me2src-tree-host> ssh <scidb>@<origin-host> # and give the password unless you did example 1 
      scidb@origin-host> bash -x /tmp/pswdless_ssh.sh <target-host-list> # all the non-coordinator hosts in cluster
      
==== end passwordless ssh =====


