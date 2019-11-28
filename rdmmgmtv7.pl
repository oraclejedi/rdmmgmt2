#!/usr/bin/perl 
#
# VMware Perl SDK LUN management tool
#
# Version History
# 
# 11-FEB-2015 - GCT - 1.0.1 - initial version - fork of William Lam's lunManagement.pl code
# 21-MAY-2015 - GCT - 1.0.2 - rescan issued to all nodes of esx cluster
# 15-JUL-2015 - GCT - 1.0.3 - checks to ensure devices are attached after rescan
# 10-NOV-2015 - GCT - 1.0.5 - added storage refresh after hba rescan and allowed cluster to be named in args
# 29-DEC-2015 - GCT - 1.1.0 - rescan of hbas now option for cases where that has been done manually, test mode
# 09-JAN-2017 - GCT - 2.0.0 - delete state information added, reattach remoed, file system refresh removed

use strict;
use warnings;
use POSIX;
use VMware::VILib;
use VMware::VIRuntime;
use File::Basename;

#
# global variables
#
my $nogo = -1;
my $okay = 1;
my $script_error = "rdmmgmt critical error";
my $param_debug_level = 2;

my ($caAction,$caClusterName,$caESXHostName,$caRescan,$caVMName,$gplstAllClusters,$nResult,$nSCSI,$pVMView,$pESXHost);

#
# output mDebug infomration depending on the mDebug level set in the config file
# 

sub mDebug($$){

  my ($level,$message) = @_;
  my $indent = "";

  if( $level > $param_debug_level ){ return( $nogo ); }

  while( $level>0 ) { $indent = "${indent}   "; $level-- }

  printf "${indent}${message}\n";

  # debug returns nogo allowing errors to return a message and then exit
  # positives ignore this
  return( $nogo );
  
} # mDebug

#
# taking the esx host name
# determine the esx cluster the host is part of
# and return its name
# otherwise return null
#

sub fQueryClusterName {

  my ($esx_hostname) = @_;
  
  # step through the list of ESX clusters found
  foreach my $pESXCluster (@$gplstAllClusters) {

    # pull the cluster name
    my $caClusterName = $pESXCluster->name;
    
    my $plstESXHosts = Vim::get_views(mo_ref_array => $pESXCluster->host);
      
    foreach my $pESXHost (@$plstESXHosts) { if($pESXHost->name eq $esx_hostname) { return( $caClusterName ); }}
    
  } # end foreach

  return "";
  
} # fQueryClusterName

#
# rescan the HBAs of all servers in the ESX cluster
# warning - this can cause VMware to lock up
# the storage view refresh call is supposed to mitigate that problem
# google "rescan-o-death" to see the problem
#

sub mHBARescan($) {

  my ($cluster_name) = @_;
  
  # issue a rescan of the HBAs 
  mDebug( 0, "rescanning HBAs" );

  # step through the list of esx clusters in the data center
  foreach my $pESXCluster (@$gplstAllClusters) {

    # if this cluster matches the name of the one supplied
    if ( $pESXCluster->name eq $cluster_name ) {

      my $plstESXHosts = Vim::get_views(mo_ref_array => $pESXCluster->host);

      # step through each host in the cluster
      foreach my $pESXHost (@$plstESXHosts) {

        mDebug( 1, "cluster:".$pESXCluster->name." host found:".$pESXHost->name );

        # get a pointer to the storage subsystem of the host
        my $pStorage = Vim::get_view( mo_ref => $pESXHost->configManager->storageSystem ); 

        # issue a rescan of the HBAs 
        mDebug( 1, "rescanning HBAs on host ".$pESXHost->name );
        eval { $pStorage->RescanAllHba( ); };
        if($@) { mDebug( 0, "$script_error: HBA rescan failed" ); }
        
      } # end foreach - esx hosts on cluster
    } # end if
  } # end foreach - esx clusters
  
} # mHBARescan

#
# attach the specified luns back to the esx hosts in the cluster
# this allows us to undo an earlier detach to remount the same snapshot
# as before
#

sub mRDMAttach($$@) {

  my ($cluster_name, $vm_name, @naa_list) = @_;

  mDebug( 0, "verifying LUNs are attached and mounted to the ESX hosts" );
  
  my $nStatus = $okay;
  
  # step through the list of esx clusters in the data center
  foreach my $pESXCluster (@$gplstAllClusters) {

    # if this cluster matches the name of the one supplied
    if ( $pESXCluster->name eq $cluster_name ) {

      my $plstESXHosts = Vim::get_views(mo_ref_array => $pESXCluster->host);

      # step through each host in the cluster
      foreach my $pESXHost (@$plstESXHosts) {

        mDebug( 1, "cluster:".$pESXCluster->name." host:".$pESXHost->name );

        # get a pointer to the storage subsystem of the host
        my $pStorage = Vim::get_view( mo_ref => $pESXHost->configManager->storageSystem ); 

        # query a list of all scsi devices connected to this ESX host
	my $plstDevices = eval{$pStorage->storageDeviceInfo->scsiLun || [] };
	foreach my $pDevice (@$plstDevices) {

          # step through the list of devices looking to see if they match the NAAs passed as an argument
          if( $pDevice->canonicalName =~ m/^naa.([0-9a-f]+)$/i && grep /^$1$/, @naa_list ) { 
	      
	    mDebug( 2, "device:".$pDevice->canonicalName." state:".$pDevice->operationalState->[0]." uuid:".lc( $pDevice->uuid ));

            if( $pDevice->operationalState->[0] =~ m/off/i ) {
            
              mDebug( 0, "lun ".$pDevice->canonicalName." is not attached to host ".$pESXHost->name.", trying to attach it now" ) ;
              
              # try attaching the LUN UUID to the hosts storage system        
              eval { $pStorage->AttachScsiLun( lunUuid => lc( $pDevice->uuid )); };

              if($@) { 
          
                mDebug( 0, "warning: unable to attach lun - UUID ".lc( $pDevice->uuid ));
                $nStatus = $nogo;
          
              } else {
         
                mDebug( 2, "lun successfully attached" );
          
              } # end else 
              
            } # end if status is off
	  } # end if naa exists in the list
        } # end for all devices found on the esx server								
      } # end foreach - esx hosts on cluster
    } # end if
  } # end foreach - esx clusters

  return( $nStatus );
  
} # mRDMAttach

#
# detach the specified NAAs from every ESX host in the cluster
# this prevents an All-Paths-Down error condition on ESX hosts
# that were not running the guest VM
# such conditions cause the cluster to lock up or crash
#

sub mRDMDetach($$@) {

  my ($cluster_name, $vm_name, @naa_list) = @_;
  
  my $nStatus = $okay;
    
  # step through the list of esx clusters in the data center
  foreach my $pESXCluster (@$gplstAllClusters) {

    if( $pESXCluster->name eq $cluster_name ) {

      my $plstESXHosts = Vim::get_views(mo_ref_array => $pESXCluster->host);

      foreach my $pESXHost (@$plstESXHosts) {

        mDebug( 1, "cluster:".$pESXCluster->name." host:".$pESXHost->name );

        my $pStorage = Vim::get_view( mo_ref => $pESXHost->configManager->storageSystem ); 
        my $plstDevices = eval{$pStorage->storageDeviceInfo->scsiLun || []};
  
        foreach (@$plstDevices) {

          if( lc($_->uuid) =~ m/^[0-9a-f]{10}([0-9a-f]{16})/ && grep /^$1$/, @naa_list ) { 
          
            mDebug( 1, "detaching lun " . $_->canonicalName );
 
 	    eval { $pStorage->DetachScsiLun( lunUuid => lc($_->uuid) ); };
  
            if($@) { 
          
              mDebug( 0, "warning: unable to detach lun - UUID " . lc($_->uuid) ); 
              $nStatus = $nogo;
                
            } else {
          
              mDebug( 1, "deleting lun state " . $_->canonicalName );
              
              eval { $pStorage->DeleteScsiLunState( lunCanonicalName => $_->canonicalName ); };
              
              if($@) { 
              
                mDebug( 0, "warning: lun detached but unable to delete lun state - canonical name " . $_->canonicalName ); 
                $nStatus = $nogo;
            
              } else {
              
                mDebug( 2, "lun successfully detached and lun state deleted" );

              } # end else           
            } # end else 
          } # end if
        } # end for each - devices on esx host\        
      } # end foreach - esx hosts on cluster
    } # end if
  } # end foreach - esx clusters

  return( $nStatus );
  
} # mRDMDetach

#
# mRDMUnMap
# Passed : VM object and a list of one or more NAA's to unmap from that object
#


sub mRDMUnMap($@) {

  my ($vmview, @naa_list) = @_;

  mDebug( 0, "unmapping RDMs from virtual machine" );
  
  my $pConfigSpecOp = VirtualDeviceConfigSpecOperation->new('remove');
  my $pConfigFileOp = VirtualDeviceConfigSpecFileOperation->new('destroy');

  my @lstDevSpecs;
  my $plstDevices = $vmview->config->hardware->device;
  
  foreach (@$plstDevices) {
    
    if ($_->isa('VirtualDisk') && $_->backing->isa('VirtualDiskRawDiskMappingVer1BackingInfo')) {
        
      if( lc($_->backing->lunUuid) =~ m/^[0-9a-f]{10}([0-9a-f]{16})/ && grep /^$1$/, @naa_list ) {

        mDebug( 1, "LUN ID:".lc($_->backing->lunUuid) );
      
        push @lstDevSpecs, VirtualDeviceConfigSpec->new( 
          operation => $pConfigSpecOp, 
          device => $_, 
          fileOperation => $pConfigFileOp );
        
      } # end if
    } # end if
  } # end foreach

  # if we found matchine devices to remove from the guest VM
  if (@lstDevSpecs) {
    
    my $pVMSpec = VirtualMachineConfigSpec->new(deviceChange => \@lstDevSpecs );
    
    eval {
    
      mDebug( 0, "reconfiguring VM" );
      $vmview->ReconfigVM( spec => $pVMSpec );
      mDebug( 0, "successfully removed ".scalar(@lstDevSpecs)." RDM(s) from VM" );
      
    };
    
    # catch error conditions
    if($@) { return( mDebug( 0, "$script_error:".$@ )); }
       
  } else {

    mDebug( 0, "no mapped LUNs found matching any NAA passed" );
    return( 0 );

  } # end else

  return( scalar(@lstDevSpecs) );
  
} # mRDMUnMap


#
# mRDMMap
# Passed : VM object, SCSI controller number, and a list of one or more NAA's to map to that object
#

sub mRDMMap($$$@) {

  my ($cluster_name, $esx_host, $vmview, $scsi_controller, @naa_list) = @_;

  mDebug( 0, "mapping RDMs to virtual machine using SCSI controller " . $scsi_controller );
  
  my $pSCSIController;
  my $caVMFSPath;
  my $plstDevices = $vmview->config->hardware->device;

  foreach ( @naa_list ) { mDebug( 2, "mapping naa:$_" ); }
  
  # determine the datastore and path currently used by the vm
  foreach (@$plstDevices) {
    
    # if this matches the specified SCSI controller
    $pSCSIController = $_->key if ( $_->deviceInfo->label eq "SCSI controller ".$scsi_controller );
    
    if ($_->isa('VirtualDisk')) {

      mDebug( 2, "existing RDM mapping file:" . $_->backing->fileName );
      
      $caVMFSPath = dirname $_->backing->fileName;
      mDebug( 5, "vmfs datastore and path:" . dirname $_->backing->fileName );
      mDebug( 5, "file:" . basename $_->backing->fileName );

    } # end if
  } # end foreach
    
  # trap non existent SCSI controller
  if (!$pSCSIController) { return( mDebug( 0, "unable to find SCSI controller $scsi_controller" )); }

  # pull a list of available disks and see if they match our list of NAAs
  my $pDataStore = Vim::get_view( mo_ref => $esx_host->configManager->datastoreSystem );
  my @lstDevSpecs;
  my (%hDeviceName, %hDeviceUUID, %hDeviceSize);

  eval {

    my $lstDisks = $pDataStore->QueryAvailableDisksForVmfs( );
    
    foreach(@$lstDisks) {

      mDebug( 2, "available disk for vmfs:" . $_->devicePath  );
     
      if ($_->devicePath =~ m%/naa.([0-9a-f]+)$% && grep /^$1$/, @naa_list ) {

        mDebug( 4, "device uuid:" . $_->uuid );

        $hDeviceName{$1} = $_->deviceName;
        $hDeviceUUID{$1} = $_->uuid;
        $hDeviceSize{$1} = (($_->capacity->blockSize * $_->capacity->block)/1024);
 
      } # end if
    } # end foreach
  };

  # create the RDM mapping files in the VMFS datastore
  my $nUnique=-1;  
	
  foreach (keys %hDeviceName) {
  
    mDebug( 4, "device name " . $hDeviceName{$_} . " unique:" . $nUnique );
    mDebug( 3, "vmdk mapping file will be " . $caVMFSPath . "/naa" . $_ . ".vmdk" );
 
    my $pDiskInfo = VirtualDiskRawDiskMappingVer1BackingInfo->new(
      compatibilityMode => "physicalMode", 
      deviceName => $hDeviceName{$_}, 
      lunUuid => $hDeviceUUID{$_}, 
      diskMode => "persistent", 
      fileName => $caVMFSPath . "/naa" . $_ . ".vmdk" 
    );
    
    my $pDisk = VirtualDisk->new(
      controllerKey => $pSCSIController, 
      unitNumber => -1, 
      key => $nUnique--, 
      backing => $pDiskInfo, 
      capacityInKB => $hDeviceSize{$_}
    );

    push @lstDevSpecs, VirtualDeviceConfigSpec->new(
      operation => VirtualDeviceConfigSpecOperation->new('add'), 
      device => $pDisk, 
      fileOperation => VirtualDeviceConfigSpecFileOperation->new('create')
    );
  
  } # end foreach

  # if we added devices reconfigure the vm
  if (@lstDevSpecs) {

    my $pVMSpec = VirtualMachineConfigSpec->new( deviceChange => \@lstDevSpecs );

    eval {
    
      mDebug( 0, "reconfiguring virtual machine" );
      $vmview->ReconfigVM( spec => $pVMSpec );      
      mDebug( 0, "successfully added ".scalar(@lstDevSpecs)." RDM(s)" );
      
    };

    # catch error conditions
    if($@) { return( mDebug( 0, "$script_error:".$@ )); }
    
  } else {
  
    mDebug( 0, "no mapped LUNs found matching any NAA passed" );
    return( $okay );
    
  } # end else
  
  return( $okay );
  
} # mRDMMap

#
# load the naa file
# this allows the dba to list the NAAs in a separate file
# including the Getopt::ArgvFile was too complicated given what VMware had already decided in the SDK
# this simply tests each NAA in the list to see if it is a file, and then loads it if it is
# naa list files are optionally generated by xvolmake.pl when the naa_file option is used
#

sub mLoadNAAFile(@) {

  my (@naa_list) = @_;
  my @lstMyList = ();

  foreach my $caNAAFile (@naa_list) {

    # check if the entry is really a file  
    $caNAAFile =~ s/\s.+//g;
    mDebug( 4, "testing naa entry $caNAAFile to determine if it is a file" );
    my @lfinfo = stat $caNAAFile;
    
    if( scalar( @lfinfo ) > 0 ) {
        
       mDebug( 4, "naa entry $caNAAFile is a file" );
       
       # open and read the contents of the naa include file
       open( fhIncludeFile,"<$caNAAFile" )|| die "$script_error: cannot open naa file $caNAAFile for reading\n";
       my @caIncludeFile = <fhIncludeFile>;
       close fhIncludeFile;
  
       # read each line, discard comments and look for a directive "naa" 
       # then take everything after the equal or space and treat as a list of naas
       foreach (@caIncludeFile) {
  
        # strip npc from the output
        $_ =~ s/\n|\r|\f//g;
        $_ =~ s/^\s+|\s+$//g;
  
        #  if this is not a comment line and it is not empty
        if( $_ !~ m/^\#/ and length($_) > 0 ) { 
  
          # take before and after the separator which can be equal or a space
          my $caParam = (split /=| /, $_)[0]; 
          my $caArg = (split /=| /, $_)[1]; 

          mDebug( 4, "*". $caParam . "*" . $caArg . "*" );
          
          # if the directive is naa, then take the argument as an naa list
          if( $caParam =~ m/naa/i ) { 
          
            my @lstNAA = split(/[,:]/, $caArg );

            # copy the naa list to the return list
            foreach my $caNAA ( @lstNAA ) { push( @lstMyList, $caNAA ); } 
          
          } # end if
        } # end if
      } # end foreach

    } else {
    
       mDebug( 4, "naa entry $caNAAFile is not a file" );
       push( @lstMyList, $caNAAFile );
       
    } # end else
    
  } # end foreach

  return( @lstMyList );
  
} # mLoadNAAFile

#
# main block
#

# option processing

my %opts = (
   action => {
      type => "=s",
      help => "[map|unmap|rescan|test]",
      required => 1,      
   },
   clustername => {
         type => "=s",
         help => "name of ESX cluster",
         required => 0,
   },
   controller => {
         type => "=i",
         help => "SCSI controller to add RDMs on (default 0)",
         required => 0,
   },
   rescan => {
      type => "=s",
      help => "[yes|no] (default no)",
      required => 0,
   },
   vmname => {
      type => "=s",
      help => "name of VM",
      required => 1,
   },
   naa => {
      type => "=s",
      help => "list of NAA values to map/unmap (comma or colon separated)",
      required => 0,
   },
   debug_level => {
      type => "=i",
      help => "debug level",
      required => 0,
   },
);

#
# validate options
#
Opts::add_options(%opts);
Opts::parse();
Opts::validate();

#
# get the key arguments
#
$caAction = Opts::get_option('action');
$caClusterName = Opts::get_option('clustername') || "";
$caRescan = Opts::get_option('rescan') || "no";
$caVMName = Opts::get_option('vmname');
$nSCSI = Opts::get_option('controller') || '0';
$param_debug_level = Opts::get_option('debug_level') || '0';

#
# process the list of naas if passed - not needed for rescan or test
#
my @naa = ();
if( Opts::get_option('naa')) { @naa = split(/[,:]/, Opts::get_option('naa')); }
@naa = mLoadNAAFile( @naa );
foreach my $str ( @naa ) { mDebug( 2, "naa:$str" ); }
if( $caAction =~ m/^map$|^unmap$|^reattach$/i && 1 > scalar( @naa ) ) { die "$script_error: naa list required when action is map, unmap or reattach\n"; }

$nResult = $okay;

#
# connect to the vcenter server
#
Util::connect();

#
# verify VM name is valid
#
$pVMView = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {"config.name" => $caVMName});
unless (defined $pVMView){ die "$script_error: no virtual machine named \"$caVMName\" found\n"; }
mDebug( 0, "located virtual machine:$caVMName" );

#
# if no clustername was given we have to find it based on the vmname
#
if( $caClusterName eq "" ) {

  #retrieve the server which the VM is hosted on
  $pESXHost = Vim::get_view(mo_ref => $pVMView->runtime->host);
  unless (defined $pESXHost){ die "unable to retrieve ESX host for \"$caVMName\"\n"; }
  $caESXHostName = $pESXHost->name;

  mDebug( 0, "$caVMName is hosted on $caESXHostName" );

  # query for a list of esx cluster entities and populate the global list
  $gplstAllClusters = Vim::find_entity_views(view_type => 'ClusterComputeResource');
  $caClusterName = fQueryClusterName( $caESXHostName );
 
} # end if

mDebug( 0, "cluster is $caClusterName" ); 

#
# rescan the hbas if the action is rescan, map or reattach and the rescan flag is set to on
# note that setting the rescan flag to off will disable a rescan even if the action is rescan
#
if( $caAction =~ m/^rescan$|^map$/i ) {

  # issue a rescan of the HBAs 
  if( $caRescan =~ m/^ON$|^YES$|^TRUE$/i ) { 
  
    mHBARescan( $caClusterName ); 
    
  } else {
  
    mDebug( 0, "HBA rescan is disabled - set rescan to YES for HBA rescan" );
  
  } # end if
} # end if

#
# determine action based on the action parameter - can be map, unmap, reattach or recan
#

if( $caAction =~ m/^map$/i ) {

  # check the NAAs are attached, they will not auto-attach if previously unmapped
  $nResult = mRDMAttach( $caClusterName, $pVMView, @naa ); 
  
  if( $nResult == $okay ) { 
  
    # reconfigure the vm to add the new RDMs
    $nResult = mRDMMap( $caClusterName, $pESXHost, $pVMView, $nSCSI, @naa );

  } # end if
  
} elsif( $caAction =~m/^unmap$/i ) {

  # during an unmap operation RDMs are removed from a vm
  # the rescan operation is not integrated into this step
  # as the array must remove the unmapped volumes from the initiator group
  # before the hba rescan, otherwise the rescan will still find the volumes
  
  # unmap the devices matching the naa list
  $nResult = mRDMUnMap( $pVMView, @naa );

  # if devices were unmapped they need to be detached from every host in the esx cluster
  if( $nResult > 0 ) { 
  
    mDebug( 0, "detaching devices from all nodes of ESX cluster" );
  
    mRDMDetach( $caClusterName, $pVMView, @naa ); 
 
  } # end if
 
} elsif( $caAction =~ m/^test$/i ) {

  # this is basically a do-nothing option to test that the connectivity to the vcenter server is ok
  mDebug( 0, "connection test completed" );
  
} elsif( $caAction !~ m/^rescan$/i ) {

  die "unknown action - \"$caAction\"\n";

} # end else

#
# disconnect from the vcenter server
#
Util::disconnect();

exit(0);
