cmake_minimum_required(VERSION 3.10)

##########################################################
#  C++20
##########################################################
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

##########################################################
#  include (여기가 핵심)
##########################################################
include_directories(
  ${PROJECT_SOURCE_DIR}/../include
  /opt/DIS/include
  /opt/DIS/include/sisci
)

##########################################################
# lib 경로
##########################################################
link_directories(/opt/DIS/lib64)
link_directories(/opt/DIS/lib)

##########################################################
# Core tools
##########################################################
set(CORE_TOOLS
  sst_dump.cc
  ldb.cc
)

foreach(src ${CORE_TOOLS})
  get_filename_component(exename ${src} NAME_WE)

  add_executable(${exename}
    ${src}
  )

  target_link_libraries(${exename}
    rocksdb
  )
endforeach()

##########################################################
# Optional tools
##########################################################
if(WITH_TOOLS)

  set(TOOLS
    db_sanity_test.cc
    write_stress.cc
    db_repl_stress.cc
    dump/rocksdb_dump.cc
    dump/rocksdb_undump.cc
  )

  foreach(src ${TOOLS})
    get_filename_component(exename ${src} NAME_WE)

    add_executable(${exename}
      ${src}
    )

    target_link_libraries(${exename}
      rocksdb
    )
  endforeach()

  ##########################################################
  # NTB worker
  ##########################################################
  add_executable(nfs_compaction_worker
    nfs_compaction_worker.cc
  )

  target_link_libraries(nfs_compaction_worker
    rocksdb
    pthread
    sisci
  )
  target_include_directories(nfs_compaction_worker PRIVATE
	  /opt/DIS/include
	  /opt/DIS/include/dis
  )

endif()
