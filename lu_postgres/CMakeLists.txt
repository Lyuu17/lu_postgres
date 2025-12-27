
add_library(lu_postgres SHARED
    "src/dllmain.cpp"
    "src/funcs.cpp")

target_include_directories(lu_postgres SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(lu_postgres libpqxx::pqxx)

install(TARGETS lu_postgres RUNTIME DESTINATION "bin" COMPONENT lu_postgres)
