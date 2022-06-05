use image::ImageBuffer;
use image::Rgba;
use pathfinder_geometry::transform2d::Transform2F;
use pathfinder_rasterize::Rasterizer;
use pdf::file::File as PDFile;

use pdf_render::{render_page, Cache, SceneBackend};

pub enum PDFQuailty {
    High,
    Medium,
    Low,
}

pub fn render_preview_page(
    data: &[u8],
    quailty: PDFQuailty,
) -> ImageBuffer<Rgba<u8>, Vec<u8>> {
    let transform = match quailty {
        PDFQuailty::High => Transform2F::from_scale(5.),
        PDFQuailty::Medium => Transform2F::from_scale(150. / 32.4),
        PDFQuailty::Low => Transform2F::from_scale(1.),
    };
    let file = PDFile::from_data(data.to_vec()).unwrap();
    let page = file.get_page(0).as_deref().unwrap().to_owned();
    let mut cache = Cache::new();
    let mut backend = SceneBackend::new(&mut cache);
    render_page(&mut backend, &file, &page, transform)
        .expect("cannot render page");
    let img_raw = Rasterizer::new().rasterize(backend.finish(), None);
    img_raw
}

#[test]
fn test_pdf_generate() {
    use std::{fs::File, io::Read};
    let mut pdf_reader = File::open("tests/test.pdf").unwrap();

    let mut bytes = Vec::new();
    pdf_reader.read_to_end(&mut bytes).unwrap();

    let img = render_preview_page(bytes.as_slice(), PDFQuailty::Low);
    img.save("tests/test.png")
        .expect("cannot save image");
}
