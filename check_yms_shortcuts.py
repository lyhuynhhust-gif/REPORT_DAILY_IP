import os
import glob

def get_desktop_path():
    return os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

def check_yms_shortcuts():
    desktop = get_desktop_path()
    # Tìm các file .appref-ms
    shortcuts = glob.glob(os.path.join(desktop, "*.appref-ms"))
    
    print("\n" + "="*80)
    print(f"{'TÊN SHORTCUT':<40} | {'ĐỊA CHỈ SERVER / URL'}")
    print("-" * 80)
    
    results = []
    for shortcut in shortcuts:
        name = os.path.basename(shortcut)
        try:
            # ClickOnce files thường là UTF-16LE
            with open(shortcut, 'r', encoding='utf-16') as f:
                content = f.read()
                # Lấy phần URL trước dấu #
                url = content.split('#')[0] if '#' in content else content
                results.append((name, url.strip()))
        except Exception as e:
            results.append((name, f"Lỗi không đọc được: {e}"))
            
    # Sắp xếp cho dễ nhìn
    results.sort()
    
    for name, url in results:
        # Làm sạch hiển thị
        display_name = name.replace(".appref-ms", "")
        print(f"{display_name:<40} | {url}")
        
    print("="*80 + "\n")
    print("Ghi chú từ Hiền Đệ:")
    print(" - Các bản có IP '192.168.x.x' thường là bản NỘI BỘ (Chuẩn).")
    print(" - Các bản có IP '172.16.x.x' thường là bản dành cho VINA.")

if __name__ == "__main__":
    check_yms_shortcuts()
