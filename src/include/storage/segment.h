#pragma once
#include "buffer/buffer_manager.h"
namespace buzzdb {

class Segment {
    public:
    /// Constructor.
    /// @param[in] segment_id       Id of the segment.
    /// @param[in] buffer_manager   The buffer manager that should be used by the segment.
    Segment(uint16_t segment_id, BufferManager& buffer_manager)
        : segment_id(segment_id), buffer_manager(buffer_manager) {}

    protected:
    /// The segment id
    uint16_t segment_id;
    /// The buffer manager
    BufferManager& buffer_manager;
};

}  // namespace buzzdb
